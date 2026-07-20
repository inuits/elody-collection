from collections.abc import Callable
from typing import Any, TypedDict

CreatorReturnType = dict[str, str | dict[str, list[dict[str, str]]]]
ElodyDocumentSerializationReturnType = dict[str, str | dict[str, list[dict[str, str]]]]
SerializeToElodyReturnType = dict[str, str | dict[str, list[dict[str, str]]]]
DocumentContentPatcherReturnType = dict[str, str | dict[str, list[dict[str, str]]]]
PreCrudHookReturnType = dict[str, str | dict[str, list[dict[str, str]]]]
PostCrudHookReturnType = None

class CrudDict(TypedDict):
    creator: Callable[..., CreatorReturnType]
    collection: str
    collection_history: str
    document_content_patcher: Callable[..., DocumentContentPatcherReturnType]
    post_crud_hook: Callable[..., PostCrudHookReturnType]
    pre_crud_hook: Callable[..., PreCrudHookReturnType]
    content_changes_checker: Callable[..., bool]

class DocumentInfoDict(TypedDict):
    etag_key: str
    object_lists: dict[str, str]

class ObjectConfiguration:
    def crud(self) -> CrudDict: ...
    def document_info(self) -> DocumentInfoDict: ...
    def serialization(
        self, from_format: str, to_format: str
    ) -> Callable[..., ElodyDocumentSerializationReturnType]: ...
    SCHEMA_TYPE: str

class ObjectConfigurationMapper:
    def __init__(self, mapper: dict[str, Any] = ...) -> None: ...
    def get(self, key: str, schema: str | None = ...) -> ObjectConfiguration: ...
    def get_all(self) -> dict[str, Any]: ...
