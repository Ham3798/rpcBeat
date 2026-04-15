// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { IIdentityRegistry } from "../src/IIdentityRegistry.sol";
import { FoundryCheats } from "./FoundryCheats.sol";

contract RegisterRPCBeatAgent is FoundryCheats {
    address internal constant BSC_TESTNET_IDENTITY_REGISTRY =
        0x8004A818BFB912233c491871b3d84c89A494BD9e;

    string internal constant DEFAULT_AGENT_URI =
        "https://raw.githubusercontent.com/Ham3798/rpcBeat/main/agent/rpcbeat-agent.json";

    event RPCBeatAgentRegistered(
        uint256 indexed agentId,
        address indexed registry,
        string agentURI
    );

    function run() external returns (uint256 agentId) {
        address registry = VM.envOr("ERC8004_IDENTITY_REGISTRY", BSC_TESTNET_IDENTITY_REGISTRY);
        string memory agentURI = VM.envOr("RPCBEAT_AGENT_URI", DEFAULT_AGENT_URI);

        VM.startBroadcast();
        agentId = IIdentityRegistry(registry).register(agentURI);
        VM.stopBroadcast();

        emit RPCBeatAgentRegistered(agentId, registry, agentURI);
    }
}
