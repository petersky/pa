"""Registration context for the stdio proxy; never boots a service or store."""

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.core.context import AppContext
from pa.core.hooks import HookBus
from pa.domain.instance_config import merge_config_into_settings


class UnavailableStore:
    def __getattr__(self, name):
        raise RuntimeError("Stdio MCP must access service state through the owner API")


def registration_context() -> AppContext:
    # Reading the small instance configuration preserves standalone CLI and
    # assigned capability derivation. Do not call get_settings: it creates
    # directories and may persist a new session secret.
    initial = Settings()
    values = {"data_dir": initial.data_dir}
    merge_config_into_settings(initial.data_dir, values)
    settings = Settings(**values)
    return AppContext(
        settings=settings,
        hooks=HookBus(),
        store=UnavailableStore(),
        services={"async_runtime": AsyncRuntime()},
    )
