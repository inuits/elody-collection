class BaseObjectMigrator:
    def __init__(self, *, status, silent=False):
        self._status = status
        self._silent = silent

    @property
    def status(self):
        return self._status

    @property
    def silent(self):
        return self._silent

    def bulk_migrate(self, object_configuration_mapper, *, dry_run=False):
        pass

    def lazy_migrate(self, item, *, dry_run=False):
        return item
