from __future__ import annotations

import unittest

import httpx

from pa.http_client import PAClient, PAHTTPError


class PAHTTPClientTests(unittest.TestCase):
    def test_cookie_client_rotates_csrf_and_retries_idempotent_mutation(self) -> None:
        posts: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(
                    200,
                    headers={"Set-Cookie": "pa_csrf=token-one; Path=/"},
                    json={"ok": True},
                )
            posts.append(request.headers.get("X-CSRF-Token"))
            if len(posts) == 1:
                return httpx.Response(
                    403,
                    headers={"Set-Cookie": "pa_csrf=token-two; Path=/"},
                    json={
                        "detail": {
                            "code": "csrf_expired",
                            "message": "rotated",
                        }
                    },
                )
            return httpx.Response(200, json={"accepted": True})

        with PAClient(
            "https://pa.test", transport=httpx.MockTransport(handler)
        ) as client:
            response = client.request(
                "POST",
                "/api/mutate",
                idempotency_key="mutation-1",
                json={"value": 1},
            )

        self.assertTrue(response.json()["accepted"])
        self.assertEqual(posts, ["token-one", "token-two"])

    def test_unsafe_request_without_idempotency_is_not_retried(self) -> None:
        posts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal posts
            if request.method == "GET":
                return httpx.Response(
                    200, headers={"Set-Cookie": "pa_csrf=token; Path=/"}
                )
            posts += 1
            return httpx.Response(
                403,
                json={"detail": {"code": "csrf_expired", "message": "expired"}},
            )

        with (
            PAClient(
                "https://pa.test", transport=httpx.MockTransport(handler)
            ) as client,
            self.assertRaises(PAHTTPError) as raised,
        ):
            client.request("POST", "/api/mutate", json={})
        self.assertEqual(raised.exception.code, "csrf_expired")
        self.assertEqual(posts, 1)

    def test_peer_routing_is_explicit_and_cross_origin_paths_are_rejected(self) -> None:
        hosts: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            hosts.append(request.url.host)
            return httpx.Response(200, json={"ok": True})

        with PAClient(
            "https://local.test",
            peer_urls={"peer": "https://peer.test"},
            bearer_token="redacted",
            transport=httpx.MockTransport(handler),
        ) as client:
            client.request("GET", "/api/status", instance_id="peer")
            with self.assertRaises(ValueError):
                client.request("GET", "https://other.test/api/status")
        self.assertEqual(hosts, ["peer.test"])


if __name__ == "__main__":
    unittest.main()
