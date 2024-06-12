from os import path
from sys import argv, path as sys_path

sys_path.append(path.dirname(path.dirname(path.abspath(__file__))))
from configuration import init_mappers, get_object_configuration_mapper
from logging_elody.log import log


class BulkMigrator:
    EXCEPTION_LIMIT = 2

    def __init__(self):
        self.exception_count = 0

    def __call__(self, type):
        self.exception_count = 0
        config = get_object_configuration_mapper().get(type)
        if config.migration().status == "disabled":
            return

        while True:
            if self.exception_count >= self.EXCEPTION_LIMIT:
                break

            try:
                config.migration().bulk_migrate(dry_run=True)
                config.migration().bulk_migrate(
                    dry_run=config.migration().status == "dry_run"
                )
            except Exception as exception:
                log.exception(
                    f"{exception.__class__.__name__}: {exception}",
                    {},
                    exc_info=exception,
                )
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
    init_mappers()
    migrate = BulkMigrator()
    migrate(argv[1])
