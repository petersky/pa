"""Account presentation, return destinations, and credential boundaries."""
from html.parser import HTMLParser
from urllib.parse import parse_qs, quote, urlsplit

import pytest
from fastapi.testclient import TestClient

from pa.auth.csrf import generate_token
from tests.test_local_operator_acceptance import (
    operator_app, configure_operator, csrf, make_card, sign_in,
)


class Elements(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.elements = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))

    def one(self, tag, **attrs):
        return next(a for t, a in self.elements if t == tag and all(a.get(k) == v for k, v in attrs.items()))


@pytest.mark.parametrize("path", ["/", "/work?realm=default", "/settings?section=instance"])
def test_shell_uses_browser_provenance_instead_of_local_username(operator_app, path):
    app = operator_app
    with TestClient(app) as client:
        guest = client.get(path)
        link = Elements(guest.text).one("a", **{"data-sign-in": None, "aria-label": "Sign in; browsing without sign-in"})
        assert parse_qs(urlsplit(link["href"]).query)["next"] == [path]
        assert "Not signed in" in guest.text and "data-sign-out" not in guest.text
        configure_operator(client, app)
        sign_in(client)
        signed = client.get(path)
        assert 'aria-label="Account: local"' in signed.text
        assert "Local User" in signed.text and 'action="/logout"' in signed.text
        assert "Not signed in" not in signed.text
        # Authenticated transport or agent provenance is not a human browser.
        user = app.state.ctx.require_service("users").get("local")
        for headers in [{"Authorization": "Bearer " + user.cli_token},
                        {"X-PA-MCP-Instance-ID": "test-agent"}]:
            response = client.get(path, headers=headers)
            assert "Not signed in" in response.text and "data-sign-out" not in response.text


def test_card_action_and_failed_login_retain_original_destination(operator_app):
    with TestClient(operator_app) as client:
        card = make_card(operator_app)
        html = client.get(f"/partials/cards/{card.id}/detail").text
        link = Elements(html).one("a", **{"data-sign-in-card": card.id})
        assert "Sign in to review and accept" in html
        destination = f"/?card={card.id}&realm=default"
        page = client.get(link["href"])
        assert Elements(page.text).one("input", name="next")["value"] == destination
        assert "no default password" in page.text and "pa user set-password local" in page.text
        before = operator_app.state.ctx.store.get_card(card.id)
        denied = client.post("/login", data={"username": "local", "password": "wrong",
            "csrf": client.cookies["pa_csrf"], "next": destination}, follow_redirects=False)
        query = parse_qs(urlsplit(denied.headers["location"]).query)
        assert query == {"next": [destination], "error": ["Invalid credentials"]}
        assert "pa_session" not in client.cookies
        assert operator_app.state.ctx.store.get_card(card.id) == before
        configure_operator(client, operator_app)
        page = client.get(denied.headers["location"])
        assert Elements(page.text).one("input", name="next")["value"] == destination
        success = client.post("/login", data={"username": "local", "password": "test-operator-password",
            "csrf": client.cookies["pa_csrf"], "next": destination}, follow_redirects=False)
        assert success.status_code == 303 and success.headers["location"] == destination
        assert operator_app.state.ctx.store.get_card(card.id) == before  # Login is never acceptance.


UNSAFE_RETURNS = [
    "https://hostile.test/", "//hostile.test/", "///hostile.test/", "javascript:alert(1)",
    "work", "\\\\hostile.test/", "/\\hostile.test/", "/%5chostile.test/", "/%2fhostile.test/",
    "/%252fhostile.test/", "/%255chostile.test/", "/\nhostile.test", "\t//hostile.test/",
    "/%0d%0aLocation:https://hostile.test", "/a/..//hostile.test/", "/%2e%2e//hostile.test/",
]


@pytest.mark.parametrize("destination", UNSAFE_RETURNS + ["/work?realm=default&card=test#summary", "/work?q=café"])
def test_login_and_logout_validate_return_on_both_get_and_post(operator_app, destination):
    expected = "/" if destination in UNSAFE_RETURNS else destination
    with TestClient(operator_app) as client:
        configure_operator(client, operator_app)
        page = client.get("/login", params={"next": destination})
        assert Elements(page.text).one("input", name="next")["value"] == expected
        response = client.post("/login", data={"username": "local", "password": "test-operator-password",
            "csrf": client.cookies["pa_csrf"], "next": destination}, follow_redirects=False)
        encoded = quote(expected, safe=":/%#?=@[]!$&'()*+,;")
        assert response.status_code == 303 and response.headers["location"] == encoded
        response = client.post("/logout", data={"next": destination, "csrf": client.cookies["pa_csrf"]},
            follow_redirects=False)
        assert response.status_code == 303 and response.headers["location"] == encoded
        assert "pa_session" not in client.cookies
        assert "Not signed in" in client.get("/").text


@pytest.mark.parametrize("token_kind", ["missing", "mismatch", "forged", "expired"])
def test_login_rejects_bad_csrf_without_issuing_browser_session(operator_app, token_kind):
    with TestClient(operator_app) as client:
        configure_operator(client, operator_app)
        client.get("/login")
        token = client.cookies["pa_csrf"]
        if token_kind == "missing":
            token = ""
        elif token_kind == "mismatch":
            token = "not-the-cookie"
        else:
            token = "forged" if token_kind == "forged" else generate_token(operator_app.state.ctx.settings.session_secret, now=1)
            client.cookies.clear()
            client.cookies.set("pa_csrf", token)
        response = client.post("/login", data={"username": "local", "password": "test-operator-password",
            "csrf": token, "next": "/work?card=test"}, follow_redirects=False)
        assert "Invalid+session" in response.headers["location"]
        assert "pa_session" not in client.cookies


def test_logout_requires_csrf_and_same_origin(operator_app):
    with TestClient(operator_app) as client:
        configure_operator(client, operator_app)
        sign_in(client)
        for headers in [{}, {"X-CSRF-Token": "wrong"}, csrf(client) | {"Origin": "https://hostile.test"}]:
            assert client.post("/api/auth/logout", headers=headers).status_code == 403
            assert "pa_session" in client.cookies
            assert 'aria-label="Account: local"' in client.get("/").text
        assert client.get("/api/auth/logout").status_code in {404, 405}
        assert "pa_session" in client.cookies
        assert client.post("/api/auth/logout", headers=csrf(client)).json() == {"ok": True}
        assert "Not signed in" in client.get("/").text


@pytest.mark.parametrize("token_kind", ["missing", "mismatch", "forged", "expired", "foreign-origin"])
def test_native_logout_requires_signed_csrf_and_same_origin(operator_app, token_kind):
    with TestClient(operator_app) as client:
        configure_operator(client, operator_app)
        sign_in(client)
        token = client.cookies["pa_csrf"]
        headers = {}
        if token_kind == "missing":
            token = ""
        elif token_kind == "mismatch":
            token = "wrong"
        elif token_kind == "foreign-origin":
            headers["Origin"] = "https://hostile.test"
        else:
            token = "forged" if token_kind == "forged" else generate_token(operator_app.state.ctx.settings.session_secret, now=1)
            client.cookies.delete("pa_csrf")
            client.cookies.set("pa_csrf", token)
        response = client.post("/logout", data={"csrf": token}, headers=headers, follow_redirects=False)
        assert response.status_code == 403
        assert "pa_session" in client.cookies
        assert 'aria-label="Account: local"' in client.get("/").text
