"""Storage adapters for KB service."""

from pipeline_codegen.kb_service.stores.filesystem import FilesystemObjectStore
from pipeline_codegen.kb_service.stores.sqlite import SQLiteMetadataStore

__all__ = ["FilesystemObjectStore", "SQLiteMetadataStore"]
