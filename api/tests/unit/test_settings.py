import sys
import types

import pytest
from config_access import build_config
from settings import (
    REQUIRED,
    ConfigError,
    Env,
    env_bool,
    env_int,
    env_list,
    env_str,
)


class TestOptionalValues:
    def test_unset_values_fall_back_to_their_default(self):
        env = Env({})

        assert env_bool(env, "FLAG", True) is True
        assert env_int(env, "NUMBER", 5) == 5
        assert env_list(env, "ITEMS", ["a"]) == ["a"]
        assert env_str(env, "TEXT", "default") == "default"
        assert env.missing == []

    def test_set_values_are_parsed(self):
        env = Env({"FLAG": "true", "NUMBER": "42", "ITEMS": "a,b", "TEXT": "x"})

        assert env_bool(env, "FLAG") is True
        assert env_int(env, "NUMBER") == 42
        assert env_list(env, "ITEMS") == ["a", "b"]
        assert env_str(env, "TEXT", "default") == "x"

    def test_empty_optional_bool_keeps_current_behaviour(self):
        assert env_bool(Env({"FLAG": ""}), "FLAG", True) is False

    def test_invalid_optional_int_is_collected_and_falls_back_to_default(self):
        env = Env({"NUMBER": "abc"})

        assert env_int(env, "NUMBER", 5) == 5
        assert env.invalid == [("NUMBER", "int")]


class TestRequiredValues:
    @pytest.mark.parametrize("source", [{}, {"KEY": ""}, {"KEY": "   "}])
    def test_unset_or_empty_values_are_collected_as_missing(self, source):
        for parser in (env_bool, env_int, env_list, env_str):
            env = Env(source)
            parser(env, "KEY", REQUIRED)
            assert env.missing == ["KEY"], parser.__name__

    def test_missing_values_return_typed_placeholders(self):
        env = Env({})

        assert env_bool(env, "FLAG", REQUIRED) is False
        assert env_int(env, "NUMBER", REQUIRED) == 0
        assert env_list(env, "ITEMS", REQUIRED) == []
        assert env_str(env, "TEXT", REQUIRED) == ""

    def test_set_values_are_parsed(self):
        env = Env({"FLAG": "no", "NUMBER": "7", "ITEMS": "a,b", "TEXT": "x"})

        assert env_bool(env, "FLAG", REQUIRED) is False
        assert env_int(env, "NUMBER", REQUIRED) == 7
        assert env_list(env, "ITEMS", REQUIRED) == ["a", "b"]
        assert env_str(env, "TEXT", REQUIRED) == "x"
        assert env.missing == []
        assert env.invalid == []

    @pytest.mark.parametrize("value", ["true", "1", "yes", "y", "t", " TRUE "])
    def test_required_bool_accepts_known_true_values(self, value):
        assert env_bool(Env({"FLAG": value}), "FLAG", REQUIRED) is True

    @pytest.mark.parametrize("value", ["false", "0", "no", "n", "f", " FALSE "])
    def test_required_bool_accepts_known_false_values(self, value):
        assert env_bool(Env({"FLAG": value}), "FLAG", REQUIRED) is False

    def test_required_bool_rejects_unknown_values(self):
        env = Env({"FLAG": "ture"})

        env_bool(env, "FLAG", REQUIRED)

        assert env.invalid == [("FLAG", "bool")]

    def test_required_list_ignores_blank_items(self):
        env = Env({"ITEMS": " a , ,b,"})

        assert env_list(env, "ITEMS", REQUIRED) == ["a", "b"]

    def test_required_list_with_only_separators_is_missing(self):
        env = Env({"ITEMS": " , ,"})

        env_list(env, "ITEMS", REQUIRED)

        assert env.missing == ["ITEMS"]


class TestPlainMappings:
    def test_missing_required_value_raises_immediately(self):
        with pytest.raises(ConfigError) as error:
            env_str({}, "TEXT", REQUIRED)

        assert error.value.missing == ["TEXT"]

    def test_invalid_value_raises_immediately(self):
        with pytest.raises(ConfigError) as error:
            env_int({"NUMBER": "abc"}, "NUMBER", 5)

        assert error.value.invalid == [("NUMBER", "int")]


class TestConfigError:
    def test_message_lists_missing_and_invalid_keys_sorted(self):
        error = ConfigError(
            missing=["B_KEY", "A_KEY"], invalid=[("PORT", "int"), ("FLAG", "bool")]
        )

        assert str(error) == (
            "Missing required environment variables: A_KEY, B_KEY\n"
            "Invalid environment variables: FLAG (expected bool), PORT (expected int)"
        )

    def test_message_never_contains_values(self):
        env = Env({"SECRET_PORT": "hunter2"})
        env_int(env, "SECRET_PORT", REQUIRED)

        error = ConfigError(env.missing, env.invalid)

        assert "hunter2" not in str(error)


class TestBuildConfig:
    @pytest.fixture
    def client_settings(self, monkeypatch, tmp_path):
        manifest = tmp_path / "app_list.json"
        manifest.write_text('{"fakeclient": {}}')

        module = types.SimpleNamespace(
            load_config=lambda env: {"CLIENT_KEY": env_str(env, "CLIENT_KEY", REQUIRED)}
        )
        monkeypatch.setitem(sys.modules, "apps.fakeclient.settings", module)
        return str(manifest)

    def test_reports_base_and_client_problems_in_one_error(
        self, monkeypatch, client_settings
    ):
        import settings

        original_load_config = settings.load_config

        def load_config_with_required_key(env):
            config = dict(original_load_config(env))
            config["BASE_KEY"] = env_str(env, "BASE_KEY", REQUIRED)
            config["BASE_PORT"] = env_int(env, "BASE_PORT", REQUIRED)
            return config

        monkeypatch.setattr("config_access.load_config", load_config_with_required_key)

        with pytest.raises(ConfigError) as error:
            build_config(
                {
                    "APPS_MANIFEST": client_settings,
                    "COLLECTION_API_URL": "http://collection-api",
                    "BASE_PORT": "not-a-port",
                }
            )

        assert sorted(error.value.missing) == ["BASE_KEY", "CLIENT_KEY"]
        assert error.value.invalid == [("BASE_PORT", "int")]

    def test_returns_config_when_everything_is_present(self, client_settings):
        config = dict(
            build_config(
                {
                    "APPS_MANIFEST": client_settings,
                    "COLLECTION_API_URL": "http://collection-api",
                    "CLIENT_KEY": "value",
                }
            )
        )

        assert config["CLIENT_KEY"] == "value"
        assert config["COLLECTION_API_URL"] == "http://collection-api"
