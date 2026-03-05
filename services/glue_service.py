import boto3
from typing import Dict, List

_glue = boto3.client("glue")

# Single cache — Glue API called once per container lifetime
_schema_cache: Dict[str, List[str]] = {}


def _load_cache(database_name: str) -> Dict[str, List[str]]:
    global _schema_cache
    if _schema_cache:
        return _schema_cache
    response = _glue.get_tables(DatabaseName=database_name)
    for table in response["TableList"]:
        _schema_cache[table["Name"]] = [
            col["Name"] for col in table["StorageDescriptor"]["Columns"]
        ]
    return _schema_cache


def get_glue_schema(database_name: str) -> str:
    """Formatted string for Agent 1 prompt. Backwards-compatible alias."""
    return get_glue_schema_text(database_name)


def get_glue_schema_text(database_name: str) -> str:
    """Formatted string for Agent 1 system prompt."""
    schema = _load_cache(database_name)
    text = ""
    for table_name, columns in schema.items():
        text += f"\nTable: {table_name}\nColumns:\n"
        for col in columns:
            text += f"- {col}\n"
    return text


def get_glue_schema_dict(database_name: str) -> Dict[str, List[str]]:
    """{ table_name: [col1, col2, ...] } for Agent 2 whitelist check."""
    return _load_cache(database_name)
