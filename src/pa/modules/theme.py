from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from pa.auth.cookies import use_secure_cookies
from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.preferences import AppearanceMode, get_preferences_store

THEMES_DIR = Path(__file__).resolve().parent.parent / "server" / "static" / "themes"


@dataclass(frozen=True)
class ThemeVariant:
    id: str
    label: str


@dataclass(frozen=True)
class ThemeInfo:
    id: str
    name: str
    description: str
    variants: tuple[ThemeVariant, ...]


def _load_manifest(theme_dir: Path) -> ThemeInfo | None:
    manifest_path = theme_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    data = json.loads(manifest_path.read_text())
    variants = tuple(
        ThemeVariant(id=v["id"], label=v["label"]) for v in data.get("variants", [])
    )
    return ThemeInfo(
        id=data["id"],
        name=data["name"],
        description=data.get("description", ""),
        variants=variants,
    )


def get_theme_catalog() -> list[dict]:
    themes: list[dict] = []
    if not THEMES_DIR.exists():
        return themes
    for theme_dir in sorted(THEMES_DIR.iterdir()):
        if not theme_dir.is_dir():
            continue
        info = _load_manifest(theme_dir)
        if info:
            themes.append(
                {
                    "id": info.id,
                    "name": info.name,
                    "description": info.description,
                    "variants": [{"id": v.id, "label": v.label} for v in info.variants],
                }
            )
    return themes


router = APIRouter(prefix="/ui")


def _user_id_from_request(request: Request) -> str | None:
    principal = get_principal_id(request)
    if principal.startswith("user:"):
        return principal[5:]
    return None


def _prefs_store(request: Request):
    settings = request.app.state.ctx.settings
    return get_preferences_store(settings.data_dir, user_id=_user_id_from_request(request))


class ThemePreferenceUpdate(BaseModel):
    appearance: AppearanceMode | None = None
    theme_id: str | None = None


@router.get("/themes")
def list_themes() -> list[dict]:
    return get_theme_catalog()


@router.get("/assets")
def asset_info(request: Request) -> dict:
    from pa import __version__

    assets = request.app.state.ctx.require_service("assets")
    return {
        "version": assets.version,
        "pa_version": __version__,
        "build_id": f"{__version__}+{assets.version}",
    }


@router.get("/theme")
def get_theme_preference(request: Request) -> dict:
    prefs = _prefs_store(request).load()
    assets = request.app.state.ctx.require_service("assets")
    from pa import __version__

    return {
        "theme_id": prefs.theme_id,
        "appearance": prefs.appearance.value,
        "themes": get_theme_catalog(),
        "asset_version": assets.version,
        "build_id": f"{__version__}+{assets.version}",
    }


@router.put("/theme")
def set_theme_preference(request: Request, body: ThemePreferenceUpdate) -> JSONResponse:
    store = _prefs_store(request)
    updates = body.model_dump(exclude_unset=True)
    prefs = store.update(**updates)
    response = JSONResponse(
        {
            "theme_id": prefs.theme_id,
            "appearance": prefs.appearance.value,
        }
    )
    secure = use_secure_cookies(request, request.app.state.ctx.settings)
    response.set_cookie(
        "pa_appearance",
        prefs.appearance.value,
        max_age=60 * 60 * 24 * 365,
        samesite="lax",
        secure=secure,
    )
    response.set_cookie(
        "pa_theme",
        prefs.theme_id,
        max_age=60 * 60 * 24 * 365,
        samesite="lax",
        secure=secure,
    )
    return response


class ThemeModule(Module):
    @property
    def name(self) -> str:
        return "theme"

    @property
    def version(self) -> str:
        return "0.0.1"

    @property
    def description(self) -> str:
        return "Themeable UI with light, dark, and system appearance"

    def api_routers(self):
        return [("/api", router, ["ui"])]

    def on_load(self, ctx: AppContext) -> None:
        ctx.register_service("theme_catalog", get_theme_catalog())

    def static_mounts(self):
        return []
