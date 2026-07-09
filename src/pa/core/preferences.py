from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json


class AppearanceMode(StrEnum):
    SYSTEM = "system"
    LIGHT = "light"
    DARK = "dark"


class UserPreferences(BaseModel):
    theme_id: str = "pa"
    appearance: AppearanceMode = AppearanceMode.SYSTEM
    agent_auto_approve_permissions: bool = False


class PreferencesStore:
    def __init__(self, path: Path, *, fallback_path: Path | None = None) -> None:
        self.path = path
        self.fallback_path = fallback_path

    def load(self) -> UserPreferences:
        if self.path.exists():
            return self._read(self.path)
        if self.fallback_path and self.fallback_path.exists():
            return self._read(self.fallback_path)
        return UserPreferences()

    def _read(self, path: Path) -> UserPreferences:
        try:
            data = json.loads(path.read_text())
            return UserPreferences.model_validate(data)
        except (json.JSONDecodeError, ValueError):
            return UserPreferences()

    def save(self, prefs: UserPreferences) -> UserPreferences:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.path, prefs.model_dump())
        return prefs

    def update(self, **kwargs) -> UserPreferences:
        prefs = self.load()
        updated = prefs.model_copy(update=kwargs)
        return self.save(updated)


def get_preferences_store(data_dir: Path, user_id: str | None = None) -> PreferencesStore:
    global_path = data_dir / "preferences.json"
    if not user_id:
        return PreferencesStore(global_path)
    user_path = data_dir / "users" / user_id / "preferences.json"
    return PreferencesStore(user_path, fallback_path=global_path)
