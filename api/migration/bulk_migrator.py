import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from importlib import import_module
from object_configurations.object_configuration_mapper import ObjectConfigurationMapper


class BulkMigrator:
    EXCEPTION_LIMIT = 2

    def __init__(self):
        self.exception_count = 0

    def __call__(self, type, object_configuration_mapper):
        self.exception_count = 0
        config = object_configuration_mapper.get(type)
        if config.migration().status == "disabled":
            return

        while True:
            if self.exception_count >= self.EXCEPTION_LIMIT:
                break

            try:
                config.migration().bulk_migrate(
                    object_configuration_mapper, dry_run=True
                )
                config.migration().bulk_migrate(
                    object_configuration_mapper,
                    dry_run=config.migration().status == "dry_run",
                )
            except Exception:
                self.__patch_exception_count(1)
            else:
                self.__patch_exception_count(0)
                break

    def __patch_exception_count(self, increase_count):
        if increase_count == 0:
            self.exception_count = 0
        else:
            self.exception_count += increase_count


if __name__ == "__main__":
    type = sys.argv[1]
    try:
        mapper_module = import_module("apps.mappers")
        object_configuration_mapper = ObjectConfigurationMapper(
            mapper_module.OBJECT_CONFIGURATION_MAPPER
        )
    except ModuleNotFoundError:
        object_configuration_mapper = ObjectConfigurationMapper()

    migrate = BulkMigrator()
    migrate(type, object_configuration_mapper)
