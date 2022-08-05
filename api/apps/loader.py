import app

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
    try:
        with open("apps/app_list.yaml", "r") as stream:
            try:
                apps = safe_load(stream)
            except YAMLError as ex:
                app.logger.error(f"Could not load applist: {ex}")
    except FileNotFoundError as ex:
        app.logger.error(f"Applist not found: {ex}")
    return apps
