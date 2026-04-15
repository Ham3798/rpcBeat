# RPCBeat ERC-8004 Registration

This folder contains the minimal Foundry setup used to register RPCBeat as an
ERC-8004-compatible BNB execution safety agent.

RPCBeat uses the official ERC-8004 Identity Registry already deployed on BSC
Testnet. It does not redeploy the registry and does not claim full reputation or
validation registry integration for the hackathon demo.

## Addresses

- BSC Testnet IdentityRegistry:
  `0x8004A818BFB912233c491871b3d84c89A494BD9e`
- BSC Mainnet IdentityRegistry:
  `0x8004A169FB4a3325136EB29fA0ceB6D2e539a432`

## Agent URI

```text
https://raw.githubusercontent.com/Ham3798/rpcBeat/main/agent/rpcbeat-agent.json
```

## Environment

```bash
export BSC_TESTNET_RPC_URL="https://data-seed-prebsc-1-s1.bnbchain.org:8545"
export BSC_TESTNET_PRIVATE_KEY="0x..."
export ERC8004_IDENTITY_REGISTRY="0x8004A818BFB912233c491871b3d84c89A494BD9e"
export RPCBEAT_AGENT_URI="https://raw.githubusercontent.com/Ham3798/rpcBeat/main/agent/rpcbeat-agent.json"
```

Do not commit private keys.

## Build And Test

```bash
cd contracts/erc8004
forge build
forge test
```

## Register Agent

```bash
forge script script/RegisterRPCBeatAgent.s.sol:RegisterRPCBeatAgent \
  --rpc-url "$BSC_TESTNET_RPC_URL" \
  --private-key "$BSC_TESTNET_PRIVATE_KEY" \
  --broadcast
```

After the transaction confirms, read the `Registered` event from BscScan or the
Forge broadcast log and export the `agentId`:

```bash
export RPCBEAT_AGENT_ID="..."
```

## Record Demo Metadata

This records a compact reference to the demo wallet and transaction analysis. It
is only an execution-context reference, not proof of route safety, exact RPC path,
or realized loss.

```bash
forge script script/SetRPCBeatDemoMetadata.s.sol:SetRPCBeatDemoMetadata \
  --rpc-url "$BSC_TESTNET_RPC_URL" \
  --private-key "$BSC_TESTNET_PRIVATE_KEY" \
  --broadcast
```

## Readback

```bash
cast call "$ERC8004_IDENTITY_REGISTRY" \
  "tokenURI(uint256)(string)" "$RPCBEAT_AGENT_ID" \
  --rpc-url "$BSC_TESTNET_RPC_URL"

cast call "$ERC8004_IDENTITY_REGISTRY" \
  "getMetadata(uint256,string)(bytes)" "$RPCBEAT_AGENT_ID" \
  "rpcbeat.analysis.demo" \
  --rpc-url "$BSC_TESTNET_RPC_URL"
```

