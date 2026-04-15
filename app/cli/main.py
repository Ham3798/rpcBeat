from __future__ import annotations

import json
import os
import re
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import typer

from app.config import get_settings
from app.dune.cli import DuneCli, DuneCliError
from app.dune.client import DuneClient, DuneError
from app.dune.evaluator import evaluate_rows
from app.dune.registry import QueryDefinition, QueryRegistry

app = typer.Typer(help="RPCBeat feedback loop CLI.")
query_app = typer.Typer(help="Dune query lifecycle commands.")
dune_app = typer.Typer(help="Dune CLI and agent environment commands.")
app.add_typer(query_app, name="query")
app.add_typer(dune_app, name="dune")


def require_client() -> DuneClient:
    settings = get_settings()
    if not settings.dune_api_key:
        raise typer.BadParameter("DUNE_API_KEY is required for live Dune operations.")
    return DuneClient(
        settings.dune_api_key,
        base_url=settings.dune_base_url,
        timeout_seconds=settings.rpcbeat_dune_timeout_seconds,
    )


def registry() -> QueryRegistry:
    settings = get_settings()
    return QueryRegistry(settings.rpcbeat_query_dir, settings.rpcbeat_query_registry)


@query_app.command("sync")
def sync_queries() -> None:
    """Create or update Dune queries from queries/*.sql."""
    settings = get_settings()
    reg = registry()
    definitions = reg.load_definitions()
    if not definitions:
        typer.echo("No SQL definitions found in queries/.")
        raise typer.Exit(code=1)

    existing_definitions: list[QueryDefinition] = []
    new_definitions: list[QueryDefinition] = []
    for definition in definitions:
        if reg.get_query_id(definition.key):
            existing_definitions.append(definition)
        else:
            new_definitions.append(definition)

    new_definitions.sort(
        key=lambda definition: ("research" in definition.path.parts, definition.key)
    )

    with require_client() as client:
        for definition in [*existing_definitions, *new_definitions]:
            existing_id = reg.get_query_id(definition.key)
            if existing_id:
                client.update_query(
                    existing_id,
                    name=definition.name,
                    query_sql=definition.sql,
                    description=definition.description,
                    tags=definition.tags,
                    parameters=definition.parameters,
                )
                query_id = existing_id
                action = "updated"
            else:
                query_id = client.create_query(
                    name=definition.name,
                    query_sql=definition.sql,
                    description=definition.description,
                    tags=definition.tags,
                    is_private=settings.rpcbeat_dune_private,
                    parameters=definition.parameters,
                )
                action = "created"
            reg.set_query_id(definition, query_id)
            typer.echo(f"{action}: {definition.key} -> query_id={query_id}")


@query_app.command("run")
def run_suite(suite: str = typer.Option("baseline", help="Canary suite name.")) -> None:
    """Execute registered queries with canary params and store latest results."""
    settings = get_settings()
    reg = registry()
    suite_path = settings.rpcbeat_eval_dir / "canaries" / f"{suite}.json"
    if not suite_path.exists():
        raise typer.BadParameter(f"Canary suite not found: {suite_path}")
    canaries = json.loads(suite_path.read_text())
    output_dir = settings.rpcbeat_eval_dir / "runs"
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_record: dict[str, Any] = {"run_id": run_id, "suite": suite, "queries": {}}

    definitions = {definition.key: definition for definition in reg.load_definitions()}
    with require_client() as client:
        for query_key, params in canaries.items():
            definition = definitions.get(query_key)
            query_id = reg.get_query_id(query_key)
            if not definition or not query_id:
                run_record["queries"][query_key] = {"status": "skipped", "reason": "not registered"}
                continue
            started = time.monotonic()
            try:
                execution_id = client.execute_query(query_id, parameters=params)
                status = client.wait_for_execution(
                    execution_id,
                    poll_interval_seconds=settings.rpcbeat_dune_poll_interval_seconds,
                    timeout_seconds=settings.rpcbeat_dune_timeout_seconds,
                )
                rows = client.fetch_all_rows(
                    execution_id,
                    max_rows=settings.rpcbeat_max_result_rows,
                )
                elapsed = time.monotonic() - started
                result = {
                    "status": "completed",
                    "query_id": query_id,
                    "execution_id": execution_id,
                    "elapsed_seconds": elapsed,
                    "dune_status": status,
                    "rows": rows,
                }
            except DuneError as exc:
                result = {
                    "status": "failed",
                    "query_id": query_id,
                    "error": str(exc),
                    "dune_response": exc.response,
                    "sql": definition.sql,
                }
            run_record["queries"][query_key] = result
            typer.echo(f"{query_key}: {result['status']}")

    latest_path = output_dir / "latest.json"
    run_path = output_dir / f"{run_id}.json"
    serialized = json.dumps(run_record, indent=2, sort_keys=True, default=str) + "\n"
    run_path.write_text(serialized)
    latest_path.write_text(serialized)
    typer.echo(f"Wrote {run_path}")


@query_app.command("eval")
def eval_latest() -> None:
    """Evaluate latest run against query metadata assertions."""
    settings = get_settings()
    latest_path = settings.rpcbeat_eval_dir / "runs" / "latest.json"
    if not latest_path.exists():
        raise typer.BadParameter("No latest run found. Run `rpcbeat query run` first.")
    run_record = json.loads(latest_path.read_text())
    definitions = {definition.key: definition for definition in registry().load_definitions()}
    results = []
    for query_key, query_run in run_record.get("queries", {}).items():
        definition = definitions.get(query_key)
        if not definition:
            continue
        if query_run.get("status") != "completed":
            results.append(
                {
                    "query_key": query_key,
                    "passed": False,
                    "failures": [query_run.get("error", "query did not complete")],
                    "metrics": {},
                }
            )
            continue
        evaluation = evaluate_rows(
            query_key,
            query_run.get("rows", []),
            definition.metadata,
            execution_seconds=query_run.get("elapsed_seconds"),
        )
        results.append(evaluation.__dict__)
        status = "PASS" if evaluation.passed else "FAIL"
        typer.echo(f"{status}: {query_key}")
        for failure in evaluation.failures:
            typer.echo(f"  - {failure}")

    report_path = settings.rpcbeat_eval_dir / "runs" / "latest_eval.json"
    report_path.write_text(json.dumps(results, indent=2, sort_keys=True, default=str) + "\n")
    if any(not result["passed"] for result in results):
        raise typer.Exit(code=1)


@query_app.command("improve")
def improve_prompt() -> None:
    """Create a prompt package for improving the latest failed query."""
    settings = get_settings()
    latest_path = settings.rpcbeat_eval_dir / "runs" / "latest.json"
    if not latest_path.exists():
        raise typer.BadParameter("No latest run found. Run `rpcbeat query run` first.")
    run_record = json.loads(latest_path.read_text())
    failed_key = None
    failed_run = None
    for query_key, query_run in run_record.get("queries", {}).items():
        if query_run.get("status") != "completed":
            failed_key = query_key
            failed_run = query_run
            break
    if not failed_key or not failed_run:
        typer.echo("No failed query found in latest run.")
        return

    prompt = build_improvement_prompt(failed_key, failed_run)
    output_path = settings.rpcbeat_eval_dir / "improvement_prompt.md"
    output_path.write_text(prompt)
    typer.echo(f"Wrote {output_path}")


@query_app.command("canary")
def write_default_canary() -> None:
    """Write a default baseline canary suite if it does not exist."""
    settings = get_settings()
    path = settings.rpcbeat_eval_dir / "canaries" / "baseline.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        typer.echo(f"Already exists: {path}")
        return
    end_time = datetime.now(UTC).replace(microsecond=0)
    start_time = end_time - timedelta(days=7)
    payload = default_canary_payload(start_time.isoformat(), end_time.isoformat())
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    typer.echo(f"Wrote {path}")


@query_app.command("smoke-sql")
def smoke_sql(
    query: str = typer.Option(..., "--query", help="Query key matching queries/<key>.sql."),
    params: str = typer.Option(
        "evals/canaries/baseline.json",
        "--params",
        help="JSON file with canary parameters, keyed by query name.",
    ),
) -> None:
    """Run a saved SQL template through Dune CLI run-sql with JSON output."""
    reg = registry()
    definition = definition_by_key(reg.load_definitions(), query)
    if not definition:
        raise typer.BadParameter(f"Unknown query key: {query}")
    params_path = Path(params)
    if not params_path.exists():
        raise typer.BadParameter(f"Params file not found: {params_path}")
    all_params = json.loads(params_path.read_text())
    query_params = all_params.get(query, all_params)
    if not isinstance(query_params, dict):
        raise typer.BadParameter("Params file must contain an object for the selected query.")
    sql = render_sql_template(definition.sql, query_params)
    try:
        result = DuneCli().run_sql(sql)
    except DuneCliError as exc:
        typer.echo(f"ERROR: {exc}", err=True)
        if exc.stderr:
            typer.echo(exc.stderr, err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(result, indent=2, sort_keys=True, default=str))


@dune_app.command("doctor")
def dune_doctor(
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    """Inspect optional Dune CLI, auth, and Codex skill setup."""
    settings = get_settings()
    env = os.environ.copy()
    if settings.dune_api_key:
        env["DUNE_API_KEY"] = settings.dune_api_key
    report = DuneCli(env=env).doctor().as_dict()
    if json_output:
        typer.echo(json.dumps(report, indent=2, sort_keys=True))
        return
    typer.echo(f"Dune CLI installed: {report['dune_installed']}")
    typer.echo(f"Dune CLI path: {report['dune_path'] or '-'}")
    typer.echo(f"DUNE_API_KEY env present: {report['env_api_key_present']}")
    typer.echo(f"Dune config present: {report['config_file_present']}")
    typer.echo(f"Codex Dune skill present: {report['codex_skill_present']}")
    typer.echo("Codex skill candidates:")
    for candidate in report["codex_skill_candidates"]:
        typer.echo(f"  - {candidate}")
    typer.echo(f"Runnable: {report['runnable']}")
    typer.echo(report["message"])


def build_improvement_prompt(query_key: str, query_run: dict[str, Any]) -> str:
    response = json.dumps(query_run.get("dune_response", {}), indent=2, sort_keys=True)
    sql = query_run.get("sql", "")
    return f"""# RPCBeat Query Improvement Request

Query key: `{query_key}`

The latest Dune execution failed. Propose the smallest SQL patch that fixes the
error while preserving the query output contract.

## Dune Error

```json
{response}
```

## SQL

```sql
{sql}
```
"""


def default_canary_payload(start_time: str, end_time: str) -> dict[str, Any]:
    return {
        "wallet_mev_exposure": {
            "wallet": "0x0000000000000000000000000000000000000000",
            "start_time": start_time,
            "end_time": end_time,
        },
        "wallet_builder_context": {
            "wallet": "0x0000000000000000000000000000000000000000",
            "start_time": start_time,
            "end_time": end_time,
        },
        "wallet_pool_context": {
            "wallet": "0x0000000000000000000000000000000000000000",
            "start_time": start_time,
            "end_time": end_time,
        },
        "wallet_route_context": {
            "wallet": "0x0000000000000000000000000000000000000000",
            "start_time": start_time,
            "end_time": end_time,
        },
        "wallet_gas_sponsorship_context": {
            "wallet": "0x0000000000000000000000000000000000000000",
            "start_time": start_time,
            "end_time": end_time,
        },
        "tx_execution_context": {
            "tx_hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "start_time": start_time,
            "end_time": end_time,
        },
        "builder_sandwich_exposure": {"start_time": start_time, "end_time": end_time},
        "pair_token_risk": {
            "start_time": start_time,
            "end_time": end_time,
            "token_pair_or_addresses": "WBNB-USDT",
        },
    }


def definition_by_key(definitions: list[QueryDefinition], key: str) -> QueryDefinition | None:
    for definition in definitions:
        if definition.key == key:
            return definition
    return None


def render_sql_template(sql: str, parameters: dict[str, Any]) -> str:
    missing = sorted(set(re.findall(r"\{\{([a-zA-Z0-9_]+)\}\}", sql)) - set(parameters))
    if missing:
        raise typer.BadParameter(f"Missing SQL parameters: {', '.join(missing)}")
    rendered = sql
    for key, value in parameters.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", str(value))
    return rendered


if __name__ == "__main__":
    app()
