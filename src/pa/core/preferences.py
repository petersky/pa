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


class PreferencesStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> UserPreferences:
        if not self.path.exists():
            return UserPreferences()
        try:
            data = json.loads(self.path.read_text())
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


def get_preferences_store(data_dir: Path) -> PreferencesStore:
    return PreferencesStore(data_dir / "preferences.json")
