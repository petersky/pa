"""Authenticated GitHub operations for the Add Repo dialog."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal

import httpx
from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

from pa.auth.middleware import get_principal_id, require_user
from pa.domain.models import RepositoryCreate
from pa.domain.projection import MutationOperationConflict, MutationOperationInProgress
from pa.pr_supervisor.github import GitHubAPIError, GitHubClient, GitHubCredentials
from pa.repository.github import GitHubRepositories, validate_name

router = APIRouter(prefix="/github")


async def _service(request: Request) -> GitHubRepositories:
    require_user(request)
    ctx = request.app.state.ctx
    runtime = ctx.require_service("async_runtime")
    credentials = await runtime.run_blocking(
        "filesystem.github_credentials_read", GitHubCredentials.load, ctx.settings.data_dir
    )
    if not credentials.token:
        raise HTTPException(401, "GitHub is not authenticated. Configure GitHub credentials on this PA instance.")
    return GitHubRepositories(GitHubClient(credentials, async_runtime=runtime))


def _error(exc: Exception) -> HTTPException:
    # Never reflect provider bodies, tokens, transport URLs or headers into the UI.
    if isinstance(exc, GitHubAPIError):
        if exc.status_code == 401:
            return HTTPException(401, "GitHub authentication failed. Update this instance's GitHub credentials.")
        if exc.status_code in {403, 429}:
            return HTTPException(403, "GitHub denied the request. Check token permissions or the API rate limit.")
        if exc.status_code == 422:
            return HTTPException(409, "GitHub could not create this name. It may be taken or reserved; check availability again.")
    return HTTPException(502, "GitHub could not complete the request. If creating, browse for the repository before retrying.")


@router.get("/identity")
async def identity(request: Request) -> dict:
    try:
        service = await _service(request)
        return {"authenticated": True, **await service.identity()}
    except (GitHubAPIError, httpx.HTTPError) as exc:
        raise _error(exc) from exc


@router.get("/repositories")
async def repositories(
    request: Request,
    page: int = Query(1, ge=1, le=10000),
    visibility: Literal["all", "public", "private"] = "all",
    sort: Literal["full_name", "updated", "created", "pushed"] = "full_name",
) -> dict:
    try:
        return await (await _service(request)).list(page=page, visibility=visibility, sort=sort)
    except (GitHubAPIError, httpx.HTTPError) as exc:
        raise _error(exc) from exc


@router.get("/repository-availability")
async def availability(request: Request, name: str = Query(min_length=1, max_length=100)) -> dict:
    try:
        name = validate_name(name)
        service = await _service(request)
        user = await service.identity()
        return {"name": name, "login": user["login"], "available": await service.availability(name, user["login"])}
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    except (GitHubAPIError, httpx.HTTPError) as exc:
        raise _error(exc) from exc


class NewRepository(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    confirmed_login: str = Field(min_length=1, max_length=100)
    realm: str = "default"


@router.post("/repositories", status_code=201)
async def create(
    request: Request, data: NewRepository,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=300)],
) -> dict:
    try:
        name = validate_name(data.name)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    service = await _service(request)
    ctx = request.app.state.ctx
    store = ctx.store
    run = ctx.require_service("async_runtime").run_blocking
    fingerprint = hashlib.sha256(json.dumps({
        "principal": get_principal_id(request), **data.model_dump(), "name": name,
    }, sort_keys=True).encode()).hexdigest()
    key = "github-create:" + idempotency_key
    try:
        user = await service.identity()
        if user["login"] != data.confirmed_login:
            raise HTTPException(409, "GitHub account changed. Check availability again.")
        receipt = await run(
            "repository.github_begin", store.begin_operation,
            idempotency_key=key, operation="github.create_repository",
            request_fingerprint=fingerprint, realm_id=data.realm,
        )
    except (MutationOperationConflict, MutationOperationInProgress) as exc:
        raise HTTPException(409, "This creation request is already in progress or has different details. Browse for the repository before retrying.") from exc
    except (GitHubAPIError, httpx.HTTPError) as exc:
        raise _error(exc) from exc
    if receipt and receipt.get("repository"):
        return receipt["repository"]
    if receipt is None:
        try:
            if not await service.availability(name, user["login"]):
                raise HTTPException(409, "That repository name is already taken. Choose another name or use Existing.")
            remote = await service.create(name)
        except (HTTPException, GitHubAPIError) as exc:
            await run("repository.github_failed", store.fail_operation, key, "github_request_rejected")
            if isinstance(exc, HTTPException):
                raise
            raise _error(exc) from exc
        except httpx.HTTPError as exc:
            # An ambiguous POST must not be retried automatically.
            raise _error(exc) from exc
        receipt = {"remote": remote}
        # Save GitHub success before catalog registration, so a local failure can
        # replay registration without ever issuing another GitHub creation POST.
        await run("repository.github_receipt", store.complete_operation, key, receipt)
    remote = receipt["remote"]
    try:
        repository = await run(
            "repository.github_register", store.create_repository,
            RepositoryCreate(
                realm_id=data.realm, url=remote["clone_url"], name=remote["name"],
                provider="github", provider_repository_id=str(remote["id"]),
                default_branch=remote["default_branch"],
                provider_metadata={"full_name": remote["full_name"], "private": remote["private"]},
            ),
            principal_id=get_principal_id(request), instance_id=ctx.settings.instance_id,
        )
    except Exception as exc:
        raise HTTPException(503, f"GitHub created {remote['full_name']}, but PA could not add it. Retry this request or add it from Existing.") from exc
    result = repository.model_dump(mode="json")
    await run("repository.github_registered", store.complete_operation, key, {**receipt, "repository": result})
    return result
