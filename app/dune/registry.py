from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class QueryDefinition:
    key: str
    path: Path
    sql: str
    metadata: dict[str, Any]

    @property
    def name(self) -> str:
        return str(self.metadata.get("name") or f"RPCBeat - {self.key}")

    @property
    def description(self) -> str:
        return str(self.metadata.get("description") or "")

    @property
    def tags(self) -> list[str]:
        tags = self.metadata.get("tags") or ["rpcbeat", "bnb", "mev"]
        return [str(tag) for tag in tags]

    @property
    def parameters(self) -> list[dict[str, Any]]:
        parameters = self.metadata.get("parameters") or []
        return [dict(parameter) for parameter in parameters]


class QueryRegistry:
    def __init__(self, query_dir: Path, registry_path: Path) -> None:
        self.query_dir = query_dir
        self.registry_path = registry_path
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)

    def load_definitions(self) -> list[QueryDefinition]:
        definitions: list[QueryDefinition] = []
        for path in sorted(self.query_dir.glob("*.sql")):
            metadata_path = path.with_suffix(".json")
            metadata = {}
            if metadata_path.exists():
                metadata = json.loads(metadata_path.read_text())
            definitions.append(
                QueryDefinition(
                    key=path.stem,
                    path=path,
                    sql=path.read_text(),
                    metadata=metadata,
                )
            )
        return definitions

    def load_registry(self) -> dict[str, Any]:
        if not self.registry_path.exists():
            return {"queries": {}}
        return json.loads(self.registry_path.read_text())

    def save_registry(self, registry: dict[str, Any]) -> None:
        self.registry_path.write_text(json.dumps(registry, indent=2, sort_keys=True) + "\n")

    def get_query_id(self, key: str) -> int | None:
        query = self.load_registry().get("queries", {}).get(key)
        if not query:
            return None
        query_id = query.get("query_id")
        return int(query_id) if query_id is not None else None

    def set_query_id(self, definition: QueryDefinition, query_id: int) -> None:
        registry = self.load_registry()
        queries = registry.setdefault("queries", {})
        queries[definition.key] = {
            "query_id": query_id,
            "name": definition.name,
            "path": str(definition.path),
        }
        self.save_registry(registry)
