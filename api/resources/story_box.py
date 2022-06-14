import app

from inuits_jwt_auth.authorization import current_token
from resources.base_resource import BaseResource


class StoryBox(BaseResource):
    @app.require_oauth("get-story-box")
    def get(self):
        filters = {"type": "frame", "user": current_token["Email"]}
        return self.storage.get_items_from_collection_by_fields("entities", filters)

