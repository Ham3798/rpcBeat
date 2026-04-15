// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { IIdentityRegistry } from "../src/IIdentityRegistry.sol";

contract MockIdentityRegistry is IIdentityRegistry {
    uint256 public nextAgentId = 1;
    mapping(uint256 => address) public ownerOf;
    mapping(uint256 => string) internal uris;
    mapping(uint256 => mapping(string => bytes)) internal metadata;

    event Registered(uint256 indexed agentId, string agentURI, address indexed owner);
    event MetadataSet(
        uint256 indexed agentId,
        string indexed indexedMetadataKey,
        string metadataKey,
        bytes metadataValue
    );

    function register(string calldata agentURI) external returns (uint256 agentId) {
        agentId = nextAgentId++;
        ownerOf[agentId] = msg.sender;
        uris[agentId] = agentURI;
        emit Registered(agentId, agentURI, msg.sender);
    }

    function setMetadata(
        uint256 agentId,
        string calldata metadataKey,
        bytes calldata metadataValue
    ) external {
        require(ownerOf[agentId] == msg.sender, "not owner");
        metadata[agentId][metadataKey] = metadataValue;
        emit MetadataSet(agentId, metadataKey, metadataKey, metadataValue);
    }

    function tokenURI(uint256 agentId) external view returns (string memory) {
        return uris[agentId];
    }

    function getMetadata(
        uint256 agentId,
        string calldata metadataKey
    ) external view returns (bytes memory) {
        return metadata[agentId][metadataKey];
    }
}

contract MockIdentityRegistryTest {
    function testRegisterAndMetadata() external {
        MockIdentityRegistry registry = new MockIdentityRegistry();
        string memory uri = "https://raw.githubusercontent.com/Ham3798/rpcBeat/main/agent/rpcbeat-agent.json";

        uint256 agentId = registry.register(uri);
        assert(agentId == 1);
        assert(keccak256(bytes(registry.tokenURI(agentId))) == keccak256(bytes(uri)));

        registry.setMetadata(agentId, "rpcbeat.analysis.demo", bytes("demo"));
        assert(
            keccak256(registry.getMetadata(agentId, "rpcbeat.analysis.demo"))
                == keccak256(bytes("demo"))
        );
    }
}
