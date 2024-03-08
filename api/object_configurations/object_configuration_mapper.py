from object_configurations.none_configuration import NoneConfiguration


class ObjectConfigurationMapper:
    def __init__(self, mapper={}):
        self._mapper = mapper

    def get(self, key):
        return self._mapper.get(key, NoneConfiguration)()
