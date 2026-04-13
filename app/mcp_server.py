from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from app.services.advisor import AdvisorService

mcp = FastMCP("rpcbeat")


@mcp.tool()
def analyze_wallet(wallet: str, lookback_days: int = 30) -> dict:
    """Analyze BNB DEX MEV exposure for a wallet."""
    return AdvisorService().analyze_wallet(wallet, lookback_days).model_dump(mode="json")


@mcp.tool()
def analyze_execution(tx_hash: str) -> dict:
    """Analyze execution context and MEV classification for a transaction."""
    return AdvisorService().analyze_execution(tx_hash).model_dump(mode="json")


@mcp.tool()
def explain_execution(tx_hash: str) -> dict:
    """Explain why a transaction did or did not look exposed to sandwich MEV."""
    return AdvisorService().explain_execution(tx_hash).model_dump(mode="json")


@mcp.tool()
def get_builder_mev_exposure(lookback_days: int = 7) -> dict:
    """Return builder-level sandwich exposure trend on BNB."""
    return AdvisorService().get_builder_mev_exposure(lookback_days).model_dump(mode="json")


@mcp.tool()
def get_pair_risk(token_pair_or_addresses: str, lookback_days: int = 30) -> dict:
    """Return historical sandwich exposure for a pair or token address."""
    return AdvisorService().get_pair_risk(token_pair_or_addresses, lookback_days).model_dump(
        mode="json"
    )


@mcp.tool()
def recommend_route(pair: str, amount: float, priority: str = "safe") -> dict:
    """Recommend safer execution path candidates from historical Dune observations."""
    return AdvisorService().recommend_route(pair, amount, priority).model_dump(mode="json")


if __name__ == "__main__":
    mcp.run()

