from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class EvaluationResult:
    query_key: str
    passed: bool
    failures: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)


def evaluate_rows(
    query_key: str,
    rows: list[dict[str, Any]],
    metadata: dict[str, Any],
    *,
    execution_seconds: float | None = None,
) -> EvaluationResult:
    failures: list[str] = []
    required_columns = metadata.get("required_columns") or []
    if not rows:
        failures.append("row_count must be greater than 0")
    if rows:
        available = set(rows[0].keys())
        missing = [column for column in required_columns if column not in available]
        if missing:
            failures.append(f"missing required columns: {', '.join(missing)}")

    for row_index, row in enumerate(rows):
        for column in metadata.get("non_negative_columns") or []:
            value = row.get(column)
            if value is not None and float(value) < 0:
                failures.append(f"row {row_index} column {column} is negative")
        for column in metadata.get("percentage_columns") or []:
            value = row.get(column)
            if value is not None and not (0 <= float(value) <= 1):
                failures.append(f"row {row_index} column {column} is outside 0..1")

    metrics = {
        "row_count": len(rows),
        "execution_seconds": execution_seconds,
    }
    return EvaluationResult(
        query_key=query_key,
        passed=not failures,
        failures=failures,
        metrics=metrics,
    )
