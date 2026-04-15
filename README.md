# RPCBeat

<p align="center">
  <img src="Image/rpcBeat.png" alt="RPCBeat logo" width="360">
</p>

RPCBeat is a trust-minimizing execution context layer for toxic orderflow on BNB.

It does not ask wallets or agents to blindly trust private RPCs, builder paths, or
opaque routing claims. Instead, RPCBeat turns observed BNB execution outcomes into
evidence packets: wallet orderflow, MEV exposure, transaction context,
builder-attributed block context, pool-level sandwich pressure, and historical
pair pressure. Validator/proposer context is exposed conservatively as block-level
`bnb.blocks.miner` attribution, not as intent or causality.

The goal is simple: agents should inspect execution evidence before deciding how
to route or explain a swap.

## Live Demo

- API: <https://rpcbeat-api.onrender.com>
- API docs: <https://rpcbeat-api.onrender.com/docs>
- Dune dashboard:
  <https://dune.com/ham37988/rpcbeat-bnb-toxic-orderflow-context>

Verified demo endpoints:

```bash
curl https://rpcbeat-api.onrender.com/health

curl https://rpcbeat-api.onrender.com/wallet/0x5789bcec98243e025d83039f3203b8a7e788e226

curl https://rpcbeat-api.onrender.com/tx/0xa1930dba73d2043a105cb50695b3276971283a84cacd87a94fd1c6b39d9dcfdf
```

| Before | With RPCBeat |
|---|---|
| Trust private paths | Inspect execution evidence |
| Guess route quality | Use wallet, pool, builder, and tx context |
| Score-first verdict | Metric packet for LLM reasoning |

RPCBeat separates evidence into four confidence tiers:

- `observed`: direct Dune row matches such as DEX trades and curated sandwich
  labels.
- `attributed`: block-level builder context inferred from public BNB builder
  marker/payment candidates, and block miner/proposer context from
  `bnb.blocks.miner`.
- `inferred`: route, gasless, relayed, or advisory hypotheses.
- `unknown`: fields that public Dune data cannot support, such as RPC provider
  identity, private relay admission, or validator intent.

## What RPCBeat Does

- Builds wallet orderflow metric packets from BNB DEX execution history.
- Explains a `tx_hash` with DEX trade rows, sandwich classification, block context,
  and builder-attributed block context.
- Tracks sandwich and sandwiched exposure in builder-attributed blocks over time.
- Surfaces pool and pair-level sandwich pressure for route advisory.
- Exposes FastAPI and MCP tools so agents can reason over structured evidence.

## BNB Agent Registration

RPCBeat includes a minimal ERC-8004-compatible registration path for BNB track
submission proof. The heavy execution analysis stays off-chain in the API and
Dune queries, while the agent identity and a compact demo analysis reference can
be recorded through the official ERC-8004 Identity Registry on BSC Testnet.

Current BSC Testnet proof:

- IdentityRegistry:
  [`0x8004A818BFB912233c491871b3d84c89A494BD9e`](https://testnet.bscscan.com/address/0x8004A818BFB912233c491871b3d84c89A494BD9e)
- Agent ID: `610`
- Agent registration tx:
  [`0x3a6c79afab6dd9b694b476a18169ec976276e09285c32166c784159b070881a6`](https://testnet.bscscan.com/tx/0x3a6c79afab6dd9b694b476a18169ec976276e09285c32166c784159b070881a6)
- Demo metadata tx:
  [`0x5da35db5c621b198fdae9489b05bf0828152a444c5d5c6fb6aa462f443de5dac`](https://testnet.bscscan.com/tx/0x5da35db5c621b198fdae9489b05bf0828152a444c5d5c6fb6aa462f443de5dac)
- Metadata key: `rpcbeat.analysis.demo`
- Verified readback:
  `tokenURI(610)` resolves to the RPCBeat agent metadata URI, and
  `getMetadata(610, "rpcbeat.analysis.demo")` returns the demo wallet/tx analysis
  reference.

Agent metadata:
<https://raw.githubusercontent.com/Ham3798/rpcBeat/main/agent/rpcbeat-agent.json>

Foundry scripts live in `contracts/erc8004/`:

```bash
cd contracts/erc8004
forge build
forge test

forge script script/RegisterRPCBeatAgent.s.sol:RegisterRPCBeatAgent \
  --rpc-url "$BSC_TESTNET_RPC_URL" \
  --private-key "$BSC_TESTNET_PRIVATE_KEY" \
  --broadcast

export RPCBEAT_AGENT_ID="..."

forge script script/SetRPCBeatDemoMetadata.s.sol:SetRPCBeatDemoMetadata \
  --rpc-url "$BSC_TESTNET_RPC_URL" \
  --private-key "$BSC_TESTNET_PRIVATE_KEY" \
  --broadcast
```

This is identity and execution-context attestation material only. It is not a
claim of exact RPC attribution, guaranteed route safety, full ERC-8004 reputation
integration, or validator causality.

## Dune Baseline

The hackathon baseline uses:

- 8 core queries for wallet, tx, builder, pair, route, gasless-candidate, and
  pool context.
- Archived appendix queries for chain background, experimental detectors, and
  schema/debug probes.

The public dashboard should use the core query set. Archived queries are kept
only for methodology history and are not product evidence.

## What RPCBeat Does Not Claim

- It does not identify exact RPC provider identity from public Dune data.
- It does not directly observe builder proxy admission, latency, or private relay
  policy.
- It does not treat builder payment/marker transactions as user gas sponsorship
  evidence.
- It does not claim validator intent, validator-caused harm, or counterfactual
  accounting loss.
- It does not guarantee a route. Outputs are evidence and context for agent
  reasoning.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
export DUNE_API_KEY="..."
uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000/docs` for the HTTP API.

## Public API

Core endpoints:

- `GET /health`
- `GET /wallet/{wallet}`
- `GET /tx/{tx_hash}`
- `GET /tx/{tx_hash}/explain`
- `GET /builders/exposure?lookback_days=7`
- `GET /pairs/{token_pair_or_addresses}/risk?lookback_days=30`
- `GET /recommend-route?pair=WBNB-USDT&amount=1000&priority=safe`

Demo examples:

```bash
export RPCBEAT_API_URL="https://rpcbeat-api.onrender.com"

curl "$RPCBEAT_API_URL/health"
curl "$RPCBEAT_API_URL/wallet/0x5789bcec98243e025d83039f3203b8a7e788e226"
curl "$RPCBEAT_API_URL/tx/0xa1930dba73d2043a105cb50695b3276971283a84cacd87a94fd1c6b39d9dcfdf"
```

The root path `/` is intentionally not an API endpoint. Use `/health`, `/docs`,
or the endpoint paths above.

## Deploy On Render

This repo includes `render.yaml` for a minimal FastAPI web service.

Render settings:

```bash
Build command: pip install -e .
Start command: uvicorn app.main:app --host 0.0.0.0 --port $PORT
Health check path: /health
```

Required environment variable:

```bash
DUNE_API_KEY=...
```

Optional environment variables:

```bash
RPCBEAT_DUNE_PRIVATE=true
RPCBEAT_DEFAULT_LOOKBACK_DAYS=30
RPCBEAT_MAX_RESULT_ROWS=5000
```

Current public dashboard:
<https://dune.com/ham37988/rpcbeat-bnb-toxic-orderflow-context>.

## CLI Feedback Loop

RPCBeat keeps DuneSQL queries in a repeatable feedback loop:

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

- `analyze_wallet`: wallet orderflow, MEV harm proxy, builder context, pool
  context, and RPC-path inference boundaries.
- `analyze_execution`: transaction-level sandwich classification and execution
  context.
- `explain_execution`: user/agent-readable explanation for a transaction.
- `get_builder_mev_exposure`: recent builder-level sandwich exposure.
- `get_pair_risk`: historical pair or token sandwich pressure.
- `recommend_route`: route advisory material, not live routing execution.

## Configuration

- `DUNE_API_KEY`: required for live Dune operations.
- `DUNE_BASE_URL`: defaults to `https://api.dune.com/api/v1`.
- `RPCBEAT_DUNE_PRIVATE`: defaults to `true`.
- `RPCBEAT_DEFAULT_LOOKBACK_DAYS`: defaults to `30`.
- `RPCBEAT_MAX_RESULT_ROWS`: defaults to `5000`.
- `RPCBEAT_QUERY_DIR`: defaults to `queries`.
- `RPCBEAT_QUERY_REGISTRY`: defaults to `queries/registry.json`.
- `RPCBEAT_EVAL_DIR`: defaults to `evals`.
- `RPCBEAT_DUNE_POLL_INTERVAL_SECONDS`: defaults to `2`.
- `RPCBEAT_DUNE_TIMEOUT_SECONDS`: defaults to `60`.
