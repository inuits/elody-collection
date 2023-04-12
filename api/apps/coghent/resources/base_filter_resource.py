from apps.coghent.resources.base_resource import CoghentBaseResource
from resources.base_filter_resource import BaseFilterResource


class CoghentBaseFilterResource(CoghentBaseResource, BaseFilterResource):
    def __init__(self):
        super().__init__()
