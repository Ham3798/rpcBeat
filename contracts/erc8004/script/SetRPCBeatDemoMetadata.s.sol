// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { IIdentityRegistry } from "../src/IIdentityRegistry.sol";
import { FoundryCheats } from "./FoundryCheats.sol";

contract SetRPCBeatDemoMetadata is FoundryCheats {
    address internal constant BSC_TESTNET_IDENTITY_REGISTRY =
        0x8004A818BFB912233c491871b3d84c89A494BD9e;

    string internal constant DEFAULT_METADATA_KEY = "rpcbeat.analysis.demo";

    string internal constant DEFAULT_METADATA_VALUE =
        '{"wallet":"0x5789bcec98243e025d83039f3203b8a7e788e226",'
        '"tx":"0xa1930dba73d2043a105cb50695b3276971283a84cacd87a94fd1c6b39d9dcfdf",'
        '"api":"https://rpcbeat-api.onrender.com",'
        '"dashboard":"https://dune.com/ham37988/rpcbeat-bnb-toxic-orderflow-context",'
        '"claim":"demo reference only; not route safety proof"}';

    event RPCBeatDemoMetadataSet(
        uint256 indexed agentId,
        address indexed registry,
        string metadataKey,
        bytes metadataValue
    );

    function run() external {
        address registry = VM.envOr("ERC8004_IDENTITY_REGISTRY", BSC_TESTNET_IDENTITY_REGISTRY);
        uint256 agentId = VM.envOr("RPCBEAT_AGENT_ID", uint256(0));
        string memory metadataKey = VM.envOr("RPCBEAT_METADATA_KEY", DEFAULT_METADATA_KEY);
        string memory metadataValue = VM.envOr("RPCBEAT_DEMO_METADATA", DEFAULT_METADATA_VALUE);

        require(agentId != 0, "RPCBEAT_AGENT_ID is required after registration");

        bytes memory payload = bytes(metadataValue);

        VM.startBroadcast();
        IIdentityRegistry(registry).setMetadata(agentId, metadataKey, payload);
        VM.stopBroadcast();

        emit RPCBeatDemoMetadataSet(agentId, registry, metadataKey, payload);
    }
}
