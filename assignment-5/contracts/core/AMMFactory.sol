// contracts/core/AMMFactory.sol
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "./SimpleAMM.sol";

contract AMMFactory {
    // Mapping to track pools
    mapping(address => mapping(address => address)) public getPool;
    address[] public allPools;

    event PoolCreated(address indexed token0, address indexed token1, address pool);

    function createPool(address tokenA, address tokenB) external returns (address pool) {
        require(tokenA != tokenB, "Identical tokens");
        require(tokenA != address(0) && tokenB != address(0), "Zero address");

        // Order tokens
        (address token0, address token1) = tokenA < tokenB ? (tokenA, tokenB) : (tokenB, tokenA);
        require(getPool[token0][token1] == address(0), "Factory: Pool exists");

        // Deploy new AMM
        pool = address(new SimpleAMM(token0, token1));

        // Update state
        getPool[token0][token1] = pool;
        getPool[token1][token0] = pool;
        allPools.push(pool);

        emit PoolCreated(token0, token1, pool);
    }

    function allPoolsLength() external view returns (uint) {
        return allPools.length;
    }

    // Add this function for router compatibility
    function getPoolAddress(address tokenA, address tokenB) external view returns (address) {
        (address token0, address token1) = tokenA < tokenB ? (tokenA, tokenB) : (tokenB, tokenA);
        return getPool[token0][token1];
    }

    // Alternative function name (Uniswap V2 style)
    function getPair(address tokenA, address tokenB) external view returns (address) {
        return this.getPoolAddress(tokenA, tokenB);
    }
}