# RPCBeat

RPCBeat is a hackathon baseline for an agent-facing execution advisor on BNB Chain.
It connects Dune query creation/execution with FastAPI and MCP tools so wallets and
agents can inspect MEV exposure, execution context, builder-level sandwich exposure,
and historical pair risk.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
export DUNE_API_KEY="..."
uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000/docs` for the HTTP API.

## CLI Feedback Loop

```bash
rpcbeat query sync
rpcbeat query run --suite baseline
rpcbeat query eval
rpcbeat query improve
```

`sync` creates or updates Dune queries from `queries/*.sql` and stores query ids in
`queries/registry.json`. `run` executes the baseline suite with canary parameters,
`eval` validates schemas and sanity assertions, and `improve` emits a patch prompt
from the latest failed execution.

## Optional Dune CLI Setup

Dune's CLI and Agent Skill are useful for local SQL smoke checks and agent-assisted
query debugging. They are not required by the FastAPI or MCP runtime, which uses
RPCBeat's REST API client.

```bash
curl -sSfL https://dune.com/cli/install.sh | sh
dune auth
dune --help
dune query run-sql --sql "SELECT 1 AS ok" -o json
```

The install script is expected to install the Dune CLI, prompt for authentication,
and install the Dune Agent Skill. For Codex, the manual skill location is
`~/.codex/skills/`; the documented skill install command is:

```bash
npx skills add duneanalytics/skills
```

Use RPCBeat's doctor command to inspect your local setup:

```bash
rpcbeat dune doctor
rpcbeat dune doctor --json
```

Use the Dune CLI as an optional saved-SQL smoke path:

```bash
rpcbeat query smoke-sql --query wallet_mev_exposure --params evals/canaries/baseline.json
```

Do not commit API keys. Keep `DUNE_API_KEY` in `.env`, your shell profile, or
Dune's local auth file at `~/.config/dune/config.yaml`. If a key was pasted into a
chat or log, revoke it in Dune and issue a new key.

## MCP Tools

Run the MCP server with:

```bash
python -m app.mcp_server
```

Available tools:

- `analyze_wallet`
- `analyze_execution`
- `explain_execution`
- `get_builder_mev_exposure`
- `get_pair_risk`
- `recommend_route`

## Configuration

- `DUNE_API_KEY`: required for live Dune operations.
- `RPCBEAT_DUNE_PRIVATE`: defaults to `true`.
- `RPCBEAT_DEFAULT_LOOKBACK_DAYS`: defaults to `30`.
- `RPCBEAT_MAX_RESULT_ROWS`: defaults to `5000`.
- `RPCBEAT_QUERY_REGISTRY`: defaults to `queries/registry.json`.
- `RPCBEAT_EVAL_DIR`: defaults to `evals`.
