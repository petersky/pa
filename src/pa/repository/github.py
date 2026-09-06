"""GitHub repository discovery and creation using the instance's credentials."""

from __future__ import annotations

import re
from urllib.parse import quote, urlencode

from pa.pr_supervisor.github import GitHubClient


def validate_name(name: str) -> str:
    name = name.strip()
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", name) or name in {".", ".."}:
        raise ValueError("Use 1–100 letters, numbers, periods, hyphens or underscores.")
    return name


class GitHubRepositories:
    def __init__(self, client: GitHubClient):
        self.client = client

    async def identity(self) -> dict:
        _, user = await self.client._request("GET", "/user", operation="account lookup")
        return {"login": user["login"]}

    async def list(self, *, page: int, visibility: str, sort: str) -> dict:
        query = urlencode({
            "per_page": 100, "page": page, "visibility": visibility,
            "sort": sort, "direction": "asc" if sort == "full_name" else "desc",
            "affiliation": "owner,collaborator,organization_member",
        })
        _, rows = await self.client._request(
            "GET", f"/user/repos?{query}", operation="repository listing"
        )
        return {
            "repositories": [self.summary(row) for row in rows],
            "next_page": page + 1 if len(rows) == 100 else None,
        }

    @staticmethod
    def summary(row: dict) -> dict:
        return {key: row.get(key) for key in (
            "id", "name", "full_name", "clone_url", "default_branch",
            "private", "archived", "fork", "description",
        )}

    async def availability(self, name: str, login: str) -> bool:
        name = validate_name(name)
        status, _ = await self.client._request(
            "GET", f"/repos/{quote(login, safe='')}/{quote(name, safe='')}",
            operation="name availability", allowed_statuses={200, 404},
        )
        return status == 404

    async def create(self, name: str) -> dict:
        _, row = await self.client._request(
            "POST", "/user/repos", operation="repository creation",
            json_body={"name": validate_name(name), "private": True, "auto_init": True},
            allowed_statuses={201},
        )
        return self.summary(row)
