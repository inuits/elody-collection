"""The base call sites only reach client code through the plugin contract."""

import pytest
from plugins import CollectionApiPlugin, configure_plugins, reset_plugins

queued = []


class FakeClient(CollectionApiPlugin):
    name = "fakeclient"

    def settings(self, env):
        return {"CLIENT_KEY": env.get("CLIENT_KEY", "from plugin")}

    def object_configurations(self):
        return {"asset": "AssetConfiguration"}

    def route_mapper(self):
        return {"InitJob": "/job/init"}

    def collection_mapper(self):
        return {"tickets": "abstracts"}

    def storage_mapper(self):
        return {"mongo": "ClientMongo"}

    def permissions(self):
        return {"role": {}}

    def permission_placeholders(self):
        return ["USER_IDS"]

    def register_queues(self):
        queued.append(self.name)


@pytest.fixture(autouse=True)
def fake_client(monkeypatch, tmp_path):
    manifest = tmp_path / "app_list.json"
    manifest.write_text("{}")
    monkeypatch.setenv("APPS_MANIFEST", str(manifest))
    monkeypatch.setenv("COLLECTION_API_URL", "http://collection-api")
    monkeypatch.setattr("plugins.load_plugins", lambda **kwargs: [FakeClient()])
    queued.clear()
    reset_plugins()
    yield
    configure_plugins(None)
    reset_plugins()


class TestSettings:
    def test_plugin_settings_end_up_in_the_config(self):
        from config_access import build_config

        config = dict(
            build_config(
                {
                    "APPS_MANIFEST": "/app/api/apps/app_list.json",
                    "COLLECTION_API_URL": "http://collection-api",
                }
            )
        )

        assert config["CLIENT_KEY"] == "from plugin"

    def test_plugin_settings_can_read_the_environment(self):
        from config_access import build_config

        config = dict(
            build_config(
                {
                    "APPS_MANIFEST": "/app/api/apps/app_list.json",
                    "COLLECTION_API_URL": "http://collection-api",
                    "CLIENT_KEY": "from env",
                }
            )
        )

        assert config["CLIENT_KEY"] == "from env"


class TestMappers:
    @pytest.fixture(autouse=True)
    def restore_mappers(self, monkeypatch):
        import configuration

        for name in (
            "_object_configuration_mapper",
            "_route_mapper",
            "_collection_mapper",
            "_storage_mapper",
            "_features",
        ):
            monkeypatch.setattr(configuration, name, getattr(configuration, name))
        monkeypatch.setattr(
            configuration,
            "_default_storage_mapper",
            lambda: {"mongo": "BaseMongo", "arango": "BaseArango"},
        )

    def test_mappers_come_from_the_plugins(self):
        import configuration

        configuration.init_mappers()

        assert configuration.get_object_configuration_mapper().get_all() == {
            "asset": "AssetConfiguration"
        }
        assert configuration.get_route_mapper() == {"InitJob": "/job/init"}
        assert configuration.get_collection_mapper() == {"tickets": "abstracts"}

    def test_plugin_storage_managers_override_the_base_defaults(self):
        import configuration

        configuration.init_mappers()

        assert configuration.get_storage_mapper() == {
            "mongo": "ClientMongo",
            "arango": "BaseArango",
        }

    def test_features_fall_back_to_the_base_defaults(self):
        import configuration

        configuration.init_mappers()

        assert "bulk_operations" in configuration.get_features()

    def test_plugin_features_replace_the_base_defaults(self, monkeypatch):
        import configuration

        monkeypatch.setattr(FakeClient, "features", lambda self: {"specs": {}}, False)

        configuration.init_mappers()

        assert configuration.get_features() == {"specs": {}}

    def test_init_mappers_configures_the_plugins_first(self, monkeypatch):
        # Scripts like bulk_migrator call init_mappers before anything else
        import configuration

        monkeypatch.setattr("plugins._configured", False)

        configuration.init_mappers()

        assert configuration.get_route_mapper() == {"InitJob": "/job/init"}


class TestPolicies:
    def test_permissions_and_placeholders_come_from_the_plugins(self, monkeypatch):
        import policy_factory

        calls = []
        monkeypatch.setattr(
            "elody.loader.load_policies",
            lambda factory, logger, permissions, placeholders: calls.append(
                (permissions, placeholders)
            ),
        )

        policy_factory.init_policy_factory()

        assert calls == [({"role": {}}, ["USER_IDS"])]


class TestQueues:
    def test_base_queues_load_before_the_plugin_queues(self, monkeypatch):
        import rabbit

        monkeypatch.setattr("rabbit.import_module", lambda name: queued.append(name))

        rabbit.load_queues()

        assert queued == ["resources.queues", "fakeclient"]
