// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// @dev Minimal ERC-8004 Identity Registry interface used by RPCBeat scripts.
/// The official BSC Testnet registry is used; RPCBeat does not redeploy it.
interface IIdentityRegistry {
    function register(string calldata agentURI) external returns (uint256 agentId);

    function setMetadata(
        uint256 agentId,
        string calldata metadataKey,
        bytes calldata metadataValue
    ) external;

    function tokenURI(uint256 agentId) external view returns (string memory);

    function getMetadata(
        uint256 agentId,
        string calldata metadataKey
    ) external view returns (bytes memory);
}
