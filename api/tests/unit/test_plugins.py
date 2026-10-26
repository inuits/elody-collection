import json
import sys
import types

import pytest
from plugins import (
    CollectionApiPlugin,
    LegacyAppsPlugin,
    PluginConflictError,
    concatenated,
    configure_plugins,
    first,
    get_plugins,
    load_plugins,
    merged,
    reset_plugins,
)


class Alpha(CollectionApiPlugin):
    name = "alpha"

    def object_configurations(self):
        return {"asset": "AlphaAsset"}

    def permission_placeholders(self):
        return ["A"]

    def build_schema(self, document_type, include_required):
        return {"from": "alpha"} if document_type == "asset" else None


class Beta(CollectionApiPlugin):
    name = "beta"

    def object_configurations(self):
        return {"tag": "BetaTag"}

    def permission_placeholders(self):
        return ["B"]

    def build_schema(self, document_type, include_required):
        return {"from": "beta"}


def entry_point(name, target):
    def load():
        if isinstance(target, Exception):
            raise target
        return target

    return types.SimpleNamespace(name=name, load=load)


@pytest.fixture(autouse=True)
def fresh_plugins():
    configure_plugins(None)
    reset_plugins()
    yield
    configure_plugins(None)
    reset_plugins()


class TestDefaults:
    def test_a_plugin_without_overrides_contributes_nothing(self):
        plugin = CollectionApiPlugin()

        assert plugin.settings({}) == {}
        assert plugin.object_configurations() == {}
        assert plugin.route_mapper() == {}
        assert plugin.collection_mapper() == {}
        assert plugin.storage_mapper() == {}
        assert plugin.features() == {}
        assert plugin.permissions() == {}
        assert plugin.permission_placeholders() == []
        assert plugin.blueprints() == []
        assert plugin.resource_rules() == []
        assert plugin.jobs() == []
        assert plugin.validated_document_types() == []
        assert plugin.build_schema("asset", True) is None
        assert plugin.register_queues() is None
        assert plugin.init_app(None, None) is None


class TestLoadPlugins:
    def test_instantiates_entry_points_sorted_by_name(self):
        plugins = load_plugins([entry_point("b", Beta), entry_point("a", Alpha)])

        assert [type(plugin) for plugin in plugins] == [Alpha, Beta]

    def test_rejects_entry_points_that_are_not_plugins(self):
        with pytest.raises(TypeError, match="not_a_plugin"):
            load_plugins([entry_point("not_a_plugin", dict)])

    def test_a_broken_plugin_fails_loudly(self):
        error = ImportError("broken import inside the plugin")

        with pytest.raises(ImportError):
            load_plugins([entry_point("broken", error)])

    def test_falls_back_to_the_legacy_layout_without_entry_points(self, tmp_path):
        manifest = tmp_path / "app_list.json"
        manifest.write_text("{}")

        plugins = load_plugins([], apps_manifest=str(manifest))

        assert len(plugins) == 1
        assert isinstance(plugins[0], LegacyAppsPlugin)

    def test_returns_no_plugins_without_entry_points_or_manifest(self):
        assert load_plugins([], apps_manifest=None) == []

    def test_get_plugins_refuses_to_run_before_it_is_configured(self, monkeypatch):
        monkeypatch.setattr("plugins._configured", False)
        reset_plugins()

        with pytest.raises(RuntimeError, match="configure_plugins"):
            get_plugins()

    def test_configuring_the_same_manifest_again_keeps_the_plugins(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "plugins.load_plugins", lambda **kwargs: calls.append(kwargs) or []
        )

        configure_plugins("/app/api/apps/app_list.json")
        get_plugins()
        configure_plugins("/app/api/apps/app_list.json")
        get_plugins()

        assert len(calls) == 1

    def test_get_plugins_is_cached_until_reset(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "plugins.load_plugins", lambda **kwargs: calls.append(kwargs) or [Alpha()]
        )

        get_plugins()
        get_plugins()
        reset_plugins()
        get_plugins()

        assert len(calls) == 2

    def test_configure_plugins_passes_the_manifest_and_resets(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "plugins.load_plugins", lambda **kwargs: calls.append(kwargs) or []
        )
        get_plugins()

        configure_plugins("/app/api/apps/app_list.json")
        get_plugins()

        assert calls == [
            {"apps_manifest": None},
            {"apps_manifest": "/app/api/apps/app_list.json"},
        ]


class TestCombinators:
    @pytest.fixture
    def two_plugins(self, monkeypatch):
        monkeypatch.setattr("plugins.get_plugins", lambda: [Alpha(), Beta()])

    def test_merged_combines_dicts(self, two_plugins):
        result = merged(lambda plugin: plugin.object_configurations())

        assert result == {"asset": "AlphaAsset", "tag": "BetaTag"}

    def test_merged_refuses_keys_defined_by_two_plugins(self, monkeypatch):
        class AlsoAsset(CollectionApiPlugin):
            name = "also_asset"

            def object_configurations(self):
                return {"asset": "OtherAsset"}

        monkeypatch.setattr("plugins.get_plugins", lambda: [Alpha(), AlsoAsset()])

        with pytest.raises(PluginConflictError) as error:
            merged(lambda plugin: plugin.object_configurations())

        assert "asset" in str(error.value)
        assert "alpha" in str(error.value)
        assert "also_asset" in str(error.value)

    def test_concatenated_keeps_plugin_order(self, two_plugins):
        assert concatenated(lambda plugin: plugin.permission_placeholders()) == [
            "A",
            "B",
        ]

    def test_first_returns_the_first_non_none_result(self, two_plugins):
        assert first(lambda plugin: plugin.build_schema("asset", True)) == {
            "from": "alpha"
        }
        assert first(lambda plugin: plugin.build_schema("tag", True)) == {
            "from": "beta"
        }

    def test_first_returns_none_without_plugins(self, monkeypatch):
        monkeypatch.setattr("plugins.get_plugins", list)

        assert first(lambda plugin: plugin.build_schema("asset", True)) is None


class TestLegacyAppsPlugin:
    @pytest.fixture
    def manifest(self, tmp_path):
        path = tmp_path / "app_list.json"
        path.write_text(json.dumps({"fakeclient": {"resources": ["elody", "api"]}}))
        return str(path)

    @pytest.fixture
    def fake_modules(self, monkeypatch):
        def install(name, **attributes):
            monkeypatch.setitem(sys.modules, name, types.SimpleNamespace(**attributes))

        return install

    def test_settings_come_from_each_apps_settings_module(self, manifest, fake_modules):
        fake_modules(
            "apps.fakeclient.settings",
            load_config=lambda env: {"CLIENT_KEY": env["CLIENT_KEY"]},
        )

        settings = LegacyAppsPlugin(manifest).settings({"CLIENT_KEY": "value"})

        assert settings == {"CLIENT_KEY": "value"}

    def test_an_app_without_settings_module_contributes_no_settings(self, manifest):
        assert LegacyAppsPlugin(manifest).settings({}) == {}

    def test_mappers_and_permissions_come_from_the_global_modules(self, fake_modules):
        fake_modules(
            "apps.mappers",
            OBJECT_CONFIGURATION_MAPPER={"asset": "Asset"},
            ROUTE_MAPPER={"InitJob": "/job/init"},
            COLLECTION_MAPPER={"tickets": "abstracts"},
            STORAGE_MAPPER={"mongo": "Mongo"},
            FEATURES={"specs": {}},
        )
        fake_modules(
            "apps.permissions", PERMISSIONS={"role": {}}, PLACEHOLDERS=["USER_IDS"]
        )
        plugin = LegacyAppsPlugin(None)

        assert plugin.object_configurations() == {"asset": "Asset"}
        assert plugin.route_mapper() == {"InitJob": "/job/init"}
        assert plugin.collection_mapper() == {"tickets": "abstracts"}
        assert plugin.storage_mapper() == {"mongo": "Mongo"}
        assert plugin.features() == {"specs": {}}
        assert plugin.permissions() == {"role": {}}
        assert plugin.permission_placeholders() == ["USER_IDS"]

    def test_missing_global_modules_contribute_nothing(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "apps.mappers", None)
        monkeypatch.setitem(sys.modules, "apps.permissions", None)
        plugin = LegacyAppsPlugin(None)

        assert plugin.object_configurations() == {}
        assert plugin.permissions() == {}
        assert plugin.permission_placeholders() == []

    def test_resources_are_split_into_blueprints_and_rules(
        self, manifest, fake_modules
    ):
        # Only "api_bp" was ever registered for manifest resources, keep it that way
        fake_modules(
            "apps.fakeclient.resources.elody",
            resource_rules=lambda: ["rule"],
            blueprint="never_registered",
        )
        fake_modules("apps.fakeclient.resources.api", api_bp="blueprint")
        plugin = LegacyAppsPlugin(manifest)

        assert plugin.resource_rules() == ["rule"]
        assert plugin.blueprints() == ["blueprint"]

    def test_register_queues_imports_each_apps_queue_module(
        self, manifest, monkeypatch
    ):
        imported = []
        monkeypatch.setattr("plugins.import_module", lambda name: imported.append(name))

        LegacyAppsPlugin(manifest).register_queues()

        assert imported == ["apps.fakeclient.resources.queues"]

    def test_init_app_calls_the_client_app(self, fake_modules):
        calls = []
        fake_modules("apps.client_app", init=lambda app, api: calls.append((app, api)))

        LegacyAppsPlugin(None).init_app("app", "api")

        assert calls == [("app", "api")]
