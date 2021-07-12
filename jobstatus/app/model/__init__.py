from mongoalchemy.fields import StringField

from app.config import database as data, jwt_token


class MediaFile(data.Document):
    media_id = data.StringField()
    media_file = data.StringField()


class Asset(data.Document):
    id = data.StringField()
    location = data.StringField()
    entities = data.AnythingField()


class Job(data.Document):
    job_id = data.StringField()
    job_info = data.StringField()
    owner = data.StringField()
    media_file = data.DocumentField(MediaFile)
    status = data.EnumField(StringField(), "queued", "in-progress,", "finished", "queued")
    user = data.StringField()
    asset = data.DocumentField(Asset, required=False)
