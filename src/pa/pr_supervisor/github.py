from __future__ import annotations

import hashlib
import hmac
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from pa.pr_supervisor.models import (
    GitHubCapability,
    PRCheck,
    PRPolicy,
    PRSnapshot,
    ReviewThread,
)

GITHUB_API_VERSION = "2026-03-10"
MAX_EXTERNAL_TEXT = 4000


class GitHubAPIError(RuntimeError):
    def __init__(self, status_code: int, operation: str, detail: str = "") -> None:
        safe = detail[:500].replace("\n", " ")
        super().__init__(f"GitHub {operation} failed ({status_code}): {safe}")
        self.status_code = status_code
        self.operation = operation


@dataclass
class GitHubCredentials:
    token: str = ""
    webhook_secret: str = ""
    allowed_repositories: list[str] = field(default_factory=list)
    token_source: str | None = None

    @classmethod
    def load(cls, data_dir: Path) -> GitHubCredentials:
        token = os.environ.get("PA_GITHUB_TOKEN", "").strip()
        webhook_secret = os.environ.get("PA_GITHUB_WEBHOOK_SECRET", "").strip()
        allowed: list[str] = []
        token_source = "environment" if token else None
        path = data_dir / "integrations" / "github.json"
        if path.exists():
            try:
                payload = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                payload = {}
            if not token:
                token = str(payload.get("token") or "").strip()
                if token:
                    token_source = "instance_file"
            if not webhook_secret:
                webhook_secret = str(payload.get("webhook_secret") or "").strip()
            allowed = [
                str(item).strip().strip("/")
                for item in payload.get("allowed_repositories", [])
                if str(item).strip()
            ]
        return cls(
            token=token,
            webhook_secret=webhook_secret,
            allowed_repositories=allowed,
            token_source=token_source,
        )

    def capability(self, instance_id: str) -> GitHubCapability:
        capabilities = ["pr-supervisor"]
        if self.token:
            capabilities.append("github:authenticated")
            capabilities.extend(
                f"github:repo:{repo}" for repo in self.allowed_repositories
            )
        return GitHubCapability(
            instance_id=instance_id,
            authenticated=bool(self.token),
            webhook_configured=bool(self.webhook_secret),
            token_source=self.token_source,
            allowed_repositories=self.allowed_repositories,
            capabilities=capabilities,
            state="ready" if self.token else "unauthenticated",
            detail=(
                None
                if self.token
                else "Configure PA_GITHUB_TOKEN or integrations/github.json on an eligible instance"
            ),
        )


def verify_webhook_signature(body: bytes, secret: str, signature: str | None) -> bool:
    if not secret or not signature or not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        secret.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


class GitHubClient:
    def __init__(
        self,
        credentials: GitHubCredentials,
        *,
        client: httpx.AsyncClient | None = None,
        api_url: str = "https://api.github.com",
        graphql_url: str = "https://api.github.com/graphql",
    ) -> None:
        self.credentials = credentials
        self.api_url = api_url.rstrip("/")
        self.graphql_url = graphql_url
        self._provided_client = client

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
            "User-Agent": "pa-pr-supervisor",
        }
        if self.credentials.token:
            headers["Authorization"] = f"Bearer {self.credentials.token}"
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        json_body: dict[str, Any] | None = None,
        allowed_statuses: set[int] | None = None,
    ) -> tuple[int, Any]:
        allowed_statuses = allowed_statuses or {200}
        owns = self._provided_client is None
        client = self._provided_client or httpx.AsyncClient(timeout=20.0)
        try:
            response = await client.request(
                method,
                f"{self.api_url}{path}",
                headers=self._headers(),
                json=json_body,
            )
            if response.status_code not in allowed_statuses:
                detail = ""
                try:
                    data = response.json()
                    detail = str(data.get("message") or "")
                except (ValueError, AttributeError):
                    detail = response.text[:500]
                raise GitHubAPIError(response.status_code, operation, detail)
            if response.status_code == 204:
                return response.status_code, None
            return response.status_code, response.json()
        finally:
            if owns:
                await client.aclose()

    async def probe(self, instance_id: str) -> GitHubCapability:
        capability = self.credentials.capability(instance_id)
        if not self.credentials.token:
            return capability
        try:
            await self._request("GET", "/user", operation="credential probe")
        except GitHubAPIError as exc:
            capability.authenticated = False
            capability.state = "error"
            capability.detail = str(exc)
        return capability

    async def create_pull_request(
        self,
        repository: str,
        *,
        title: str,
        head: str,
        base: str,
        body: str = "",
        draft: bool | None = None,
        policy: PRPolicy | None = None,
    ) -> dict[str, Any]:
        policy = policy or PRPolicy()
        if draft is None:
            draft = not policy.ready_by_default
        _, data = await self._request(
            "POST",
            f"/repos/{repository}/pulls",
            operation="create pull request",
            json_body={
                "title": title,
                "head": head,
                "base": base,
                "body": body,
                "draft": bool(draft),
            },
            allowed_statuses={201},
        )
        return data

    async def get_pull_head(self, repository: str, number: int) -> str:
        _, data = await self._request(
            "GET",
            f"/repos/{repository}/pulls/{number}",
            operation="get pull request head",
        )
        return str((data.get("head") or {}).get("sha") or "")

    async def snapshot(
        self, repository: str, number: int, *, policy: PRPolicy | None = None
    ) -> PRSnapshot:
        policy = policy or PRPolicy()
        _, pr = await self._request(
            "GET",
            f"/repos/{repository}/pulls/{number}",
            operation="get pull request",
        )
        head_sha = str((pr.get("head") or {}).get("sha") or "")
        base_branch = str((pr.get("base") or {}).get("ref") or "")
        if not head_sha or not base_branch:
            raise GitHubAPIError(502, "parse pull request", "missing head/base")

        protection, protection_known = await self._branch_protection(
            repository, base_branch
        )
        required_names, required_approvals = self._required_rules(
            protection, policy
        )
        checks, checks_complete = await self._checks(
            repository, head_sha, required_names
        )
        reviews, reviews_complete = await self._reviews(repository, number)
        review_data, threads, threads_known = await self._review_threads(
            repository, number
        )
        approvals = self._approval_count(reviews)
        confirmed = await self.get_pull_head(repository, number)
        merged = bool(pr.get("merged") or pr.get("merged_at"))
        state = "merged" if merged else str(pr.get("state") or "open")
        return PRSnapshot(
            repository=repository,
            number=number,
            url=str(pr.get("html_url") or f"https://github.com/{repository}/pull/{number}"),
            state=state,
            draft=bool(pr.get("draft")),
            head_sha=head_sha,
            confirmed_head_sha=confirmed,
            base_branch=base_branch,
            title=str(pr.get("title") or ""),
            mergeable=pr.get("mergeable"),
            mergeable_state=pr.get("mergeable_state"),
            merge_commit_sha=pr.get("merge_commit_sha") if merged else None,
            review_decision=review_data.get("reviewDecision"),
            approvals=approvals,
            required_approvals=(
                policy.required_approvals
                if policy.required_approvals is not None
                else required_approvals
            ),
            branch_protection_known=protection_known,
            required_checks_known=protection_known and checks_complete,
            review_threads_known=threads_known and reviews_complete,
            checks=checks,
            review_threads=threads,
            raw_urls={
                "pull_request": str(pr.get("html_url") or ""),
                "checks": f"https://github.com/{repository}/commit/{head_sha}/checks",
            },
        )

    async def _branch_protection(
        self, repository: str, branch: str
    ) -> tuple[dict[str, Any] | None, bool]:
        encoded = quote(branch, safe="")
        try:
            status, data = await self._request(
                "GET",
                f"/repos/{repository}/branches/{encoded}/protection",
                operation="get branch protection",
                allowed_statuses={200, 404},
            )
        except GitHubAPIError as exc:
            if exc.status_code == 403:
                return None, False
            raise
        if status == 404:
            return {}, True
        return data, True

    @staticmethod
    def _required_rules(
        protection: dict[str, Any] | None, policy: PRPolicy
    ) -> tuple[set[str], int]:
        required = set(policy.required_checks)
        approvals = 0
        if protection:
            status = protection.get("required_status_checks") or {}
            required.update(str(name) for name in status.get("contexts") or [])
            required.update(
                str(item.get("context"))
                for item in status.get("checks") or []
                if item.get("context")
            )
            reviews = protection.get("required_pull_request_reviews") or {}
            approvals = int(reviews.get("required_approving_review_count") or 0)
        return required, approvals

    async def _checks(
        self, repository: str, head_sha: str, required_names: set[str]
    ) -> tuple[list[PRCheck], bool]:
        _, runs = await self._request(
            "GET",
            f"/repos/{repository}/commits/{head_sha}/check-runs?per_page=100",
            operation="list check runs",
        )
        _, combined = await self._request(
            "GET",
            f"/repos/{repository}/commits/{head_sha}/status",
            operation="list commit statuses",
        )
        checks: list[PRCheck] = []
        seen: set[str] = set()
        for run in runs.get("check_runs") or []:
            name = str(run.get("name") or "unnamed")
            seen.add(name)
            output = run.get("output") or {}
            checks.append(
                PRCheck(
                    name=name,
                    status=str(run.get("status") or "queued"),
                    conclusion=run.get("conclusion"),
                    required=name in required_names,
                    details_url=run.get("details_url") or run.get("html_url"),
                    title=_bounded(output.get("title")),
                    summary=_bounded(output.get("summary")),
                    text=_bounded(output.get("text")),
                )
            )
        for status in combined.get("statuses") or []:
            name = str(status.get("context") or "status")
            if name in seen:
                continue
            state = str(status.get("state") or "pending")
            conclusion = {
                "success": "success",
                "failure": "failure",
                "error": "failure",
            }.get(state)
            checks.append(
                PRCheck(
                    name=name,
                    status="completed" if conclusion else "in_progress",
                    conclusion=conclusion,
                    required=name in required_names,
                    details_url=status.get("target_url"),
                    summary=_bounded(status.get("description")),
                )
            )
        for missing in sorted(required_names - {check.name for check in checks}):
            checks.append(
                PRCheck(
                    name=missing,
                    status="queued",
                    required=True,
                    summary="Required check has not reported for this head",
                )
            )
        run_items = runs.get("check_runs") or []
        total_runs = int(runs.get("total_count") or len(run_items))
        runs_complete = total_runs <= len(run_items)
        statuses_complete = len(combined.get("statuses") or []) < 100
        return checks, runs_complete and statuses_complete

    async def _reviews(
        self, repository: str, number: int
    ) -> tuple[list[dict[str, Any]], bool]:
        _, data = await self._request(
            "GET",
            f"/repos/{repository}/pulls/{number}/reviews?per_page=100",
            operation="list reviews",
        )
        reviews = list(data)
        return reviews, len(reviews) < 100

    @staticmethod
    def _approval_count(reviews: list[dict[str, Any]]) -> int:
        latest: dict[str, str] = {}
        for review in reviews:
            user = str((review.get("user") or {}).get("login") or "")
            state = str(review.get("state") or "").upper()
            if user and state and state != "COMMENTED":
                latest[user] = state
        return sum(1 for state in latest.values() if state == "APPROVED")

    async def _review_threads(
        self, repository: str, number: int
    ) -> tuple[dict[str, Any], list[ReviewThread], bool]:
        owner, name = repository.split("/", 1)
        query = """
        query PAReviewThreads($owner: String!, $name: String!, $number: Int!) {
          repository(owner: $owner, name: $name) {
            pullRequest(number: $number) {
              reviewDecision
              reviewThreads(first: 100) {
                pageInfo { hasNextPage }
                nodes {
                  id isResolved isOutdated path line
                  comments(last: 1) {
                    nodes { body url author { login } path line }
                  }
                }
              }
            }
          }
        }
        """
        owns = self._provided_client is None
        client = self._provided_client or httpx.AsyncClient(timeout=20.0)
        try:
            response = await client.post(
                self.graphql_url,
                headers=self._headers(),
                json={
                    "query": query,
                    "variables": {"owner": owner, "name": name, "number": number},
                },
            )
            if response.status_code != 200:
                return {}, [], False
            payload = response.json()
            if payload.get("errors"):
                return {}, [], False
            pr = (
                ((payload.get("data") or {}).get("repository") or {}).get(
                    "pullRequest"
                )
                or {}
            )
            threads: list[ReviewThread] = []
            for node in (pr.get("reviewThreads") or {}).get("nodes") or []:
                comments = (node.get("comments") or {}).get("nodes") or []
                comment = comments[-1] if comments else {}
                threads.append(
                    ReviewThread(
                        id=str(node.get("id") or ""),
                        resolved=bool(node.get("isResolved")),
                        outdated=bool(node.get("isOutdated")),
                        path=node.get("path") or comment.get("path"),
                        line=node.get("line") or comment.get("line"),
                        url=comment.get("url"),
                        author=(comment.get("author") or {}).get("login"),
                        body=_bounded(comment.get("body")) or "",
                    )
                )
            page_info = (pr.get("reviewThreads") or {}).get("pageInfo") or {}
            return pr, threads, not bool(page_info.get("hasNextPage"))
        except (httpx.HTTPError, ValueError):
            return {}, [], False
        finally:
            if owns:
                await client.aclose()


def _bounded(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text[:MAX_EXTERNAL_TEXT]
