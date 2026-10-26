"""Contract between the base collection-api and client modules.

A client module subclasses CollectionApiPlugin, overrides the hooks it needs
and registers the class as an entry point:

    [project.entry-points."elody.collection_api.plugins"]
    digipolis = "apps.digipolis.plugin:DigipolisPlugin"

The base only talks to client code through get_plugins() and the combinators
below, never through import_module("apps....").
"""

import json
from collections.abc import Callable, Iterable
from functools import cache
from importlib import import_module
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any

ENTRY_POINT_GROUP = "elody.collection_api.plugins"

_apps_manifest = None
_configured = False


class PluginConflictError(Exception):
    def __init__(self, key, owner, intruder):
        super().__init__(
            f"Plugins '{owner}' and '{intruder}' both define '{key}', "
            "only one plugin may own a key"
        )


class CollectionApiPlugin:
    """Every hook is optional, the defaults contribute nothing."""

    name = "unnamed"

    # Configuration

    def settings(self, env) -> dict:
        """Extra config values, read from env (see settings.Env)."""
        return {}

    # Mappers, these replace apps/mappers.py

    def object_configurations(self) -> dict:
        return {}

    def route_mapper(self) -> dict[str, str]:
        return {}

    def collection_mapper(self) -> dict[str, str]:
        return {}

    def storage_mapper(self) -> dict:
        return {}

    def features(self) -> dict:
        return {}

    # Permissions, these replace apps/permissions.py

    def permissions(self) -> dict:
        return {}

    def permission_placeholders(self) -> list[str]:
        return []

    # HTTP and messaging

    def blueprints(self) -> list:
        return []

    def resource_rules(self) -> list:
        return []

    def register_queues(self) -> None:
        """Import the modules that register AMQP queue handlers."""

    def jobs(self) -> list[tuple[Any, str]]:
        """(job, crontab expression) pairs for the background scheduler."""
        return []

    # Validation

    def validated_document_types(self) -> list[str]:
        """Document types this plugin can build a schema for."""
        return []

    def build_schema(self, document_type: str, include_required: bool) -> dict | None:
        """The JSON schema for document_type, or None if this plugin has none."""
        return None

    # Lifecycle

    def init_app(self, app, api) -> None:
        """Called once after the Flask app and API are set up."""


def load_plugins(eps: Iterable | None = None, apps_manifest: str | None = None):
    """Instantiate every registered plugin, sorted by entry point name.

    Without entry points, fall back to the legacy apps/ layout so clients can
    migrate one at a time. Errors are not caught: a broken plugin should stop
    the API from starting, not silently disable part of it.
    """
    if eps is None:
        eps = entry_points(group=ENTRY_POINT_GROUP)

    plugins = []
    for ep in sorted(eps, key=lambda ep: ep.name):
        plugin_class = ep.load()
        if not (
            isinstance(plugin_class, type)
            and issubclass(plugin_class, CollectionApiPlugin)
        ):
            raise TypeError(
                f"Entry point '{ep.name}' in '{ENTRY_POINT_GROUP}' is not a "
                "CollectionApiPlugin subclass"
            )
        plugins.append(plugin_class())

    if not plugins and apps_manifest:
        plugins.append(LegacyAppsPlugin(apps_manifest))
    return plugins


def configure_plugins(apps_manifest: str | None):
    """Set the legacy manifest path, called by the configuration logic.

    The config is built more than once during boot, so the plugins are only
    reloaded when the manifest actually changes.
    """
    global _apps_manifest, _configured
    if _configured and apps_manifest == _apps_manifest:
        return
    _apps_manifest = apps_manifest
    _configured = True
    reset_plugins()


@cache
def get_plugins() -> list[CollectionApiPlugin]:
    if not _configured:
        # Without this the legacy manifest is silently ignored
        raise RuntimeError(
            "configure_plugins() must be called before get_plugins(), "
            "building the config does this"
        )
    return load_plugins(apps_manifest=_apps_manifest)


def reset_plugins():  # for tests
    get_plugins.cache_clear()


def merged(hook: Callable[[CollectionApiPlugin], dict]) -> dict:
    """Combine the dicts of all plugins, refusing keys defined twice."""
    result, owners = {}, {}
    for plugin in get_plugins():
        for key, value in hook(plugin).items():
            if key in owners:
                raise PluginConflictError(key, owners[key], plugin.name)
            owners[key] = plugin.name
            result[key] = value
    return result


def concatenated[T](hook: Callable[[CollectionApiPlugin], list[T]]) -> list[T]:
    """Combine the lists of all plugins, in plugin order."""
    return [item for plugin in get_plugins() for item in hook(plugin)]


def first[T](hook: Callable[[CollectionApiPlugin], T | None]) -> T | None:
    """The first result that is not None, in plugin order."""
    for plugin in get_plugins():
        if (result := hook(plugin)) is not None:
            return result
    return None


def _optional_import(name):
    """import_module, but None when name (or a parent package) doesn't exist.

    A ModuleNotFoundError raised from inside an existing module is a real bug
    and is re-raised.
    """
    try:
        return import_module(name)
    except ModuleNotFoundError as error:
        if error.name and (name == error.name or name.startswith(f"{error.name}.")):
            return None
        raise


class LegacyAppsPlugin(CollectionApiPlugin):
    """Adapter for clients that still use the apps/ layout + APPS_MANIFEST.

    Validation and cron jobs are not covered here, those keep using their
    existing loaders until every client has migrated.
    """

    name = "legacy_apps"

    def __init__(self, apps_manifest):
        self._apps = (
            json.loads(Path(apps_manifest).read_text()) if apps_manifest else {}
        )

    def settings(self, env):
        config = {}
        for app in self._apps:
            if module := _optional_import(f"apps.{app}.settings"):
                config.update(module.load_config(env))
        return config

    def _mapper(self, attribute):
        return getattr(_optional_import("apps.mappers"), attribute, {})

    def object_configurations(self):
        return self._mapper("OBJECT_CONFIGURATION_MAPPER")

    def route_mapper(self):
        return self._mapper("ROUTE_MAPPER")

    def collection_mapper(self):
        return self._mapper("COLLECTION_MAPPER")

    def storage_mapper(self):
        return self._mapper("STORAGE_MAPPER")

    def features(self):
        return self._mapper("FEATURES")

    def permissions(self):
        return getattr(_optional_import("apps.permissions"), "PERMISSIONS", {})

    def permission_placeholders(self):
        return getattr(_optional_import("apps.permissions"), "PLACEHOLDERS", [])

    def _resource_modules(self):
        for app, manifest in self._apps.items():
            for resource in manifest.get("resources", []):
                yield import_module(f"apps.{app}.resources.{resource}")

    def blueprints(self):
        # Only the SDK loader registered manifest blueprints, and it used
        # "api_bp". The base looked for "blueprint" but never registered it.
        return [
            module.api_bp
            for module in self._resource_modules()
            if getattr(module, "api_bp", None) is not None
        ]

    def resource_rules(self):
        rules = []
        for module in self._resource_modules():
            if resource_rules := getattr(module, "resource_rules", None):
                rules.extend(resource_rules())
        return rules

    def register_queues(self):
        for app in self._apps:
            _optional_import(f"apps.{app}.resources.queues")

    def init_app(self, app, api):
        if client_app := _optional_import("apps.client_app"):
            client_app.init(app, api)
