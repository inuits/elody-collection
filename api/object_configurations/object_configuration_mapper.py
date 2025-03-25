from object_configurations.none_configuration import NoneConfiguration


class ObjectConfigurationMapper:
    def __init__(self, mapper={}):
        self._mapper = mapper

    def get(self, key, schema=None):
        if schema:
            key = f"{schema}|{key}"
        return self._mapper.get(key, NoneConfiguration)()

    def get_all(self):
        return self._mapper
