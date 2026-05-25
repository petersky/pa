from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

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


class ThemePreferenceUpdate(BaseModel):
    appearance: AppearanceMode | None = None
    theme_id: str | None = None


@router.get("/themes")
def list_themes() -> list[dict]:
    return get_theme_catalog()


@router.get("/assets")
def asset_info(request: Request) -> dict:
    assets = request.app.state.ctx.require_service("assets")
    return {"version": assets.version}


@router.get("/theme")
def get_theme_preference(request: Request) -> dict:
    prefs = get_preferences_store(request.app.state.ctx.settings.data_dir).load()
    assets = request.app.state.ctx.require_service("assets")
    return {
        "theme_id": prefs.theme_id,
        "appearance": prefs.appearance.value,
        "themes": get_theme_catalog(),
        "asset_version": assets.version,
    }


@router.put("/theme")
def set_theme_preference(request: Request, body: ThemePreferenceUpdate) -> JSONResponse:
    store = get_preferences_store(request.app.state.ctx.settings.data_dir)
    updates = body.model_dump(exclude_unset=True)
    prefs = store.update(**updates)
    response = JSONResponse(
        {
            "theme_id": prefs.theme_id,
            "appearance": prefs.appearance.value,
        }
    )
    response.set_cookie(
        "pa_appearance",
        prefs.appearance.value,
        max_age=60 * 60 * 24 * 365,
        samesite="lax",
    )
    response.set_cookie(
        "pa_theme",
        prefs.theme_id,
        max_age=60 * 60 * 24 * 365,
        samesite="lax",
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
