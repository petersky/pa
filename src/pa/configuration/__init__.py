"""Schema-driven PA configuration."""

from pa.configuration.registry import (
    CONFIGURATION_PRECEDENCE,
    SETTINGS,
    SettingDefinition,
    get_setting,
    registry_metadata,
)

__all__ = [
    "CONFIGURATION_PRECEDENCE",
    "SETTINGS",
    "SettingDefinition",
    "get_setting",
    "registry_metadata",
]
