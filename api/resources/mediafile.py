from app import policy_factory, rabbit
from elody.util import (
    mediafile_is_public,
    signal_mediafile_changed,
    signal_mediafile_deleted,
)
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
    GenericObjectMetadata,
)


class Mediafile(GenericObject):
    @policy_factory.authenticate(RequestContext(request))
    def get(self):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        filters = {}
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        access_restricting_filters = (
            policy_factory.get_user_context().access_restrictions.filters
        )
        if isinstance(access_restricting_filters, dict):
            filters = {**filters, **access_restricting_filters}
        mediafiles = super().get("mediafiles", skip=skip, limit=limit, filters=filters)
        mediafiles["results"] = self._inject_api_urls_into_mediafiles(
            mediafiles["results"]
        )
        return mediafiles

    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        accept_header = request.headers.get("Accept")
        return super().post("mediafiles", type="mediafile", accept_header=accept_header)


class MediafileAssets(GenericObject):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        mediafile = super().get("mediafiles", id)
        entities = []
        for item in self.storage.get_mediafile_linked_entities(mediafile):
            entity = self.storage.get_item_from_collection_by_id(
                "entities", item["entity_id"].removeprefix("entities/")
            )
            entity = self._set_entity_mediafile_and_thumbnail(entity)
            entity = self._add_relations_to_metadata(entity)
            entities.append(entity)
        return self._inject_api_urls_into_entities(entities)


class MediafileCopyright(GenericObject):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        mediafile = super().get("mediafiles", id)
        if not mediafile_is_public(mediafile):
            return "none", 200
        for item in [x for x in mediafile["metadata"] if x["key"] == "rights"]:
            if "in copyright" in item["value"].lower():
                return "limited", 200
        return "full", 200


class MediafileDetail(GenericObjectDetail):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        mediafile = super().get("mediafiles", id)
        if request.args.get("raw", 0, int):
            return mediafile
        return self._inject_api_urls_into_mediafiles([mediafile])[0]

    @policy_factory.authenticate(RequestContext(request))
    def put(self, id):
        old_mediafile = super().get("mediafiles", id)
        mediafile = super().put(
            "mediafiles",
            id,
            item=old_mediafile,
            type="mediafile",
        )[0]
        signal_mediafile_changed(rabbit, old_mediafile, mediafile)
        return mediafile, 201

    @policy_factory.authenticate(RequestContext(request))
    def patch(self, id):
        old_mediafile = super().get("mediafiles", id)
        mediafile = super().patch("mediafiles", id, item=old_mediafile)[0]
        signal_mediafile_changed(rabbit, old_mediafile, mediafile)
        return mediafile, 201

    @policy_factory.authenticate(RequestContext(request))
    def delete(self, id):
        mediafile = super().get("mediafiles", id)
        linked_entities = self.storage.get_mediafile_linked_entities(mediafile)
        response = super().delete("mediafiles", id, item=mediafile)
        signal_mediafile_deleted(rabbit, mediafile, linked_entities)
        return response


class MediafileMetadata(GenericObjectDetail, GenericObjectMetadata):
    @policy_factory.authenticate(RequestContext(request))
    def patch(self, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        old_mediafile = super().get("mediafiles", id)
        metadata = super(GenericObjectDetail, self).patch(
            "mediafiles",
            id,
            item=old_mediafile,
        )[0]
        new_mediafile = self._abort_if_item_doesnt_exist("mediafiles", id)
        signal_mediafile_changed(rabbit, old_mediafile, new_mediafile)
        return metadata, 201
