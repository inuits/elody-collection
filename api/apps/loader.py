import app
import os

from importlib import import_module
from yaml import safe_load, YAMLError


def load_apps(flask_app):
    apps = parse_apps()
    for app in apps:
        for resource in apps[app].get("resources", []):
            api_bp = import_module(f"apps.{app}.resources.{resource}").api_bp
            flask_app.register_blueprint(api_bp)


def parse_apps():
    apps = dict()
    apps_manifest = os.getenv("APPS_MANIFEST")
    if not os.path.exists(apps_manifest):
        app.logger.error(f"Applist not found")
        return apps
    with open(apps_manifest, "r") as file:
        try:
            apps = safe_load(file)
        except YAMLError as ex:
            app.logger.error(f"Could not load applist: {ex}")
    return apps
