class BaseObjectMigrator:
    def bulk_migrate(self, *, dry_run=False):
        pass

    def lazy_migrate(self, item, *, dry_run=False):
        return item
