// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

interface Vm {
    function envOr(string calldata name, address defaultValue) external view returns (address);
    function envOr(string calldata name, string calldata defaultValue) external view returns (string memory);
    function envOr(string calldata name, uint256 defaultValue) external view returns (uint256);
    function startBroadcast() external;
    function stopBroadcast() external;
}

abstract contract FoundryCheats {
    address internal constant HEVM_ADDRESS =
        address(uint160(uint256(keccak256("hevm cheat code"))));

    Vm internal constant VM = Vm(HEVM_ADDRESS);
}
