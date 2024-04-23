import app

from copy import deepcopy


class Migrator:
    EXCEPTION_LIMIT = 2

    def __init__(self):
        self.exception_count = 0

    def __call__(self, original_item):
        if not isinstance(original_item, dict) or not original_item.get("type"):
            return original_item

        self.exception_count = 0
        item = deepcopy(original_item)
        latest_schema = self.__get_latest_schema(original_item["type"])

        while True:
            item_schema = self.__get_item_schema(item)
            if (
                item_schema == latest_schema
                or self.exception_count >= self.EXCEPTION_LIMIT
            ):
                break

            try:
                config = app.object_configuration_mapper.get(
                    item["type"],
                    self.__get_schema_to_upgrade_to(item_schema, latest_schema),
                )
                self.__validate_migration(
                    config.migration().lazy_migrate(deepcopy(item), dry_run=True),
                    item_schema,
                )
                migrated_item = config.migration().lazy_migrate(deepcopy(item))
            except Exception:
                self.__patch_exception_count(1)
            else:
                self.__patch_exception_count(0)
                item = migrated_item

        return item

    def __get_latest_schema(self, item_type):
        config = app.object_configuration_mapper.get(item_type)
        return f"{config.SCHEMA_TYPE}:{config.SCHEMA_VERSION}"

    def __get_item_schema(self, item):
        schema_type = item.get("schema", {}).get("type", "elody")
        schema_version = item.get("schema", {}).get("version", 1)
        return f"{schema_type}:{int(schema_version)}"

    def __validate_migration(self, migrated_item, item_schema):
        if self.__get_item_schema(migrated_item) == item_schema:
            self.__patch_exception_count(self.EXCEPTION_LIMIT)
            raise Exception(
                f"Schema version is not being updated during the migration of item with id {migrated_item.get('id', migrated_item['_id'])}."
            )

    def __get_schema_to_upgrade_to(self, item_schema, latest_schema):
        item_schema_type, item_schema_version = item_schema.split(":")
        latest_schema_type, latest_schema_version = latest_schema.split(":")
        if item_schema_type != latest_schema_type:
            raise Exception("Cannot lazily migrate to different schema types.")
        schema_version = (
            int(item_schema_version) + 1
            if int(item_schema_version) < int(latest_schema_version)
            else int(latest_schema_version)
        )
        return f"{latest_schema_type}:{schema_version}"

    def __patch_exception_count(self, increase_count):
        if increase_count == 0:
            self.exception_count = 0
        else:
            self.exception_count += increase_count
