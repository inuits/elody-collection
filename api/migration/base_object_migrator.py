class BaseObjectMigrator:
    def __init__(self, *, silent=False):
        self._silent = silent

    @property
    def silent(self):
        return self._silent

    def bulk_migrate(self, *, dry_run=False):
        pass

    def lazy_migrate(self, item, *, dry_run=False):
        return item