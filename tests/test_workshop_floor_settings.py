"""Owner defaults for the Workshop floor are settings that reach the page."""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient

from pa.config import Settings
from pa.configuration.registry import SETTINGS
from pa.core.kernel import Kernel
from pa.domain.instance_config import InstanceConfig
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent

KEYS = ("workshop_default_view", "workshop_floor_chat_agents", "workshop_floor_motion")


def test_floor_is_the_default_view_with_chat_agents_and_auto_motion() -> None:
    for model in (Settings(), InstanceConfig()):
        assert model.workshop_default_view == "floor"
        assert model.workshop_floor_chat_agents is True
        assert model.workshop_floor_motion == "auto"
    assert SETTINGS["workshop_default_view"].default == "floor"
    assert SETTINGS["workshop_default_view"].allowed == ("floor", "list")
    assert SETTINGS["workshop_floor_chat_agents"].default is True
    assert SETTINGS["workshop_floor_motion"].allowed == ("auto", "off")
    for key in KEYS:
        assert SETTINGS[key].apply == "live" and SETTINGS[key].editable


@pytest.mark.parametrize(
    "key,value", [("workshop_default_view", "grid"), ("workshop_floor_motion", "always")]
)
def test_unknown_choices_are_rejected(key: str, value: str) -> None:
    with pytest.raises(ValidationError):
        Settings(**{key: value})
    with pytest.raises(ValidationError):
        InstanceConfig(**{key: value})


def _page(**overrides: object) -> str:
    reset_store()
    reset_instance_agent()
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            data_dir=Path(tmp),
            instance_id="local",
            instance_url="http://workshop.test",
            agent_enabled=False,
            peers=[],
            **overrides,
        )
        try:
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                response = client.get("/workshop")
                assert response.status_code == 200
                return response.text
        finally:
            reset_instance_agent()
            reset_store()


def _attributes(html: str) -> dict[str, str]:
    root = re.search(r'<div id="pa-workshop-root"[^>]*>', html, re.S).group(0)
    return dict(re.findall(r'data-([a-z-]+)="([^"]*)"', root))


def test_page_carries_the_default_settings_to_the_client() -> None:
    attributes = _attributes(_page())
    assert attributes["default-view"] == "floor"
    assert attributes["floor-chat-agents"] == "true"
    assert attributes["floor-motion"] == "auto"


def test_page_carries_flipped_settings_to_the_client() -> None:
    attributes = _attributes(
        _page(
            workshop_default_view="list",
            workshop_floor_chat_agents=False,
            workshop_floor_motion="off",
        )
    )
    assert attributes["default-view"] == "list"
    assert attributes["floor-chat-agents"] == "false"
    assert attributes["floor-motion"] == "off"
