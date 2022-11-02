import os
import util

from importlib import import_module


def load_apps(flask_app):
    apps = util.read_json_as_dict(os.getenv("APPS_MANIFEST"))
    for app in apps:
        for resource in apps[app].get("resources", []):
            api_bp = import_module(f"apps.{app}.resources.{resource}").api_bp
            flask_app.register_blueprint(api_bp)
