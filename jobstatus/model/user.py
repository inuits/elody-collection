import uuid

from configuration.config import entity as user, jwt_token


class User(user.Model):
    """
    ``User - defines the user model as created in the database this is a temporary model it can be replace bey
    an appropriate data store.

    param id - primary_key, Integer , auto_increment therefore not required in request data

    param user_id - uid auto_generated not required in post data

    param user_name - string, required in request data.

    There is a one to many relationship between User model and Jobs Model.
    """

    __tablename__ = "User"
    __bind_key__ = "JOB_DB"  # maps this table to job_status database
    user_id = user.Column(user.Integer, primary_key=True)
    name = user.Column(user.String(120))
    u_id = user.Column(user.String(250), nullable=False)
    email = user.Column(user.String(255), nullable=False)
    job = user.relationship(
        "Job", backref="job_owner", lazy=True
    )  # one to many relationship mapped to Job model


# ________________________________________________ADDITIONAL-JWT-SETTINGS_______________________________________________________________________


# Use the built in identity load from Flask-Jwt-Extended
@jwt_token.user_identity_loader
def user_identity_lookup(user):
    """

    @param user:
    @return:
    """
    return user


# Register a callback function that loads a user from your database whenever
# a protected route is accessed. This should return any python object on a
# successful lookup, or None if the lookup failed for any reason (for example
# if the user has been deleted from the database).
@jwt_token.user_lookup_loader
def user_lookup_callback(_jwt_header, jwt_data):
    """

    @param _jwt_header:
    @param jwt_data:
    @return:
    """
    try:
        identity = jwt_data["sub"]
        user = User.query.filter_by(u_id=identity).one_or_none()
    except BaseException as e:
        print(e)
    return user


@jwt_token.expired_token_loader
def my_expired_token_callback(jwt_header, jwt_payload):
    """

    @param jwt_header:
    @param jwt_payload:
    @return:
    """
    return {"message": jwt_payload, "success": False}, 401
