from datetime import timedelta
from pathlib import Path

from flask.cli import load_dotenv
from flask_jwt_extended import JWTManager
import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# Load Environmental variables using dot.env
load_dotenv()
load_dotenv(load_dotenv(Path(".") / ".env"))
# initialize the main flask application
jobs = Flask(__name__)
# Disable trace modification
jobs.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
# Binding the db resources.
# you can add more than one database source here. it might be mysql, mongodb, sqlite ..etc
# the order does not matter, just make sure that you include the values in .env file.
jobs.config["SQLALCHEMY_BINDS"] = {
    # This connects to a mysql server. you may add as many endpoints as you wish to connect to the database.
    "JOB_DB": os.getenv("JOB_DB")
}
# set JWT secret
jobs.config["JWT_SECRET_KEY"] = os.environ.get("JWT_SECRET_KEY")
jobs.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1)

# temporary jwt for testing. This can work hand in hand with other security mechanisms like keycloak or Okta.
jobs.config["SECRET_KEY"] = os.getenv("FLASK_SECRET")

# set model entry point
entity = SQLAlchemy(jobs)

# set token management library this will be replaced with openID connect library
# in subsequent development steps for now it provides a base authorization and authentication model
jwt_token = JWTManager(jobs)
