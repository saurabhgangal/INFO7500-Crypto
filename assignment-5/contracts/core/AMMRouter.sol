// contracts/core/AMMRouter.sol
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "./AMMFactory.sol";
import "./SimpleAMM.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";

/**
 * @title AMMRouter
 * @author [Your Name]
 * @notice Router for easier interaction with AMM pools
 * @dev Handles token approvals and multi-hop swaps
 */
contract AMMRouter is ReentrancyGuard {
    AMMFactory public immutable factory;
    
    modifier ensure(uint256 deadline) {
        require(block.timestamp <= deadline, "Router: Expired");
        _;
    }
    
    constructor(address _factory) {
        factory = AMMFactory(_factory);
    }
    
    /**
     * @notice Add liquidity to a pool (creates pool if needed)
     */
    function addLiquidity(
        address tokenA,
        address tokenB,
        uint256 amountADesired,
        uint256 amountBDesired,
        uint256 amountAMin,
        uint256 amountBMin,
        address to,
        uint256 deadline
    ) 
        external 
        nonReentrant
        ensure(deadline) 
        returns (
            uint256 amountA, 
            uint256 amountB, 
            uint256 liquidity
        ) 
    {
        // Get or create pool
        address pool = factory.getPoolAddress(tokenA, tokenB);
        if (pool == address(0)) {
            pool = factory.createPool(tokenA, tokenB);
        }
        
        // Get token order
        (address token0, address token1) = tokenA < tokenB 
            ? (tokenA, tokenB) 
            : (tokenB, tokenA);
            
        // Determine actual amounts
        (uint256 amount0, uint256 amount1) = tokenA == token0
            ? (amountADesired, amountBDesired)
            : (amountBDesired, amountADesired);
            
        (uint256 amount0Min, uint256 amount1Min) = tokenA == token0
            ? (amountAMin, amountBMin)
            : (amountBMin, amountAMin);
        
        // Transfer tokens to router
        IERC20(token0).transferFrom(msg.sender, address(this), amount0);
        IERC20(token1).transferFrom(msg.sender, address(this), amount1);
        
        // Approve pool
        IERC20(token0).approve(pool, amount0);
        IERC20(token1).approve(pool, amount1);
        
        // Add liquidity
        (amount0, amount1, liquidity) = SimpleAMM(pool).addLiquidity(
            amount0,
            amount1,
            amount0Min,
            amount1Min,
            to,
            deadline
        );
        
        // Return actual amounts
        (amountA, amountB) = tokenA == token0
            ? (amount0, amount1)
            : (amount1, amount0);
            
        // Refund excess tokens
        uint256 excessA = tokenA == token0 ? 
            (amountADesired > amount0 ? amountADesired - amount0 : 0) :
            (amountADesired > amount1 ? amountADesired - amount1 : 0);
        uint256 excessB = tokenB == token1 ? 
            (amountBDesired > amount1 ? amountBDesired - amount1 : 0) :
            (amountBDesired > amount0 ? amountBDesired - amount0 : 0);
            
        if (excessA > 0) {
            IERC20(tokenA).transfer(msg.sender, excessA);
        }
        if (excessB > 0) {
            IERC20(tokenB).transfer(msg.sender, excessB);
        }
    }
    
    /**
     * @notice Perform a token swap
     */
    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        address[] calldata path,
        address to,
        uint256 deadline
    ) 
        external 
        nonReentrant
        ensure(deadline) 
        returns (uint256[] memory amounts) 
    {
        require(path.length >= 2, "Router: Invalid path");
        amounts = new uint256[](path.length);
        amounts[0] = amountIn;
        
        // Transfer input tokens
        IERC20(path[0]).transferFrom(msg.sender, address(this), amountIn);
        
        // Perform swaps
        for (uint256 i = 0; i < path.length - 1; i++) {
            address pool = factory.getPoolAddress(path[i], path[i + 1]);
            require(pool != address(0), "Router: Pool not found");
            
            // Approve and swap
            IERC20(path[i]).approve(pool, amounts[i]);
            amounts[i + 1] = SimpleAMM(pool).swap(
                path[i],
                amounts[i],
                0, // Min amount checked at end
                i < path.length - 2 ? address(this) : to,
                deadline
            );
        }
        
        require(amounts[amounts.length - 1] >= amountOutMin, "Router: Insufficient output");
    }
    
    /**
     * @notice Get expected output for a swap path
     */
    function getAmountsOut(
        uint256 amountIn,
        address[] calldata path
    ) external view returns (uint256[] memory amounts) {
        require(path.length >= 2, "Router: Invalid path");
        amounts = new uint256[](path.length);
        amounts[0] = amountIn;
        
        for (uint256 i = 0; i < path.length - 1; i++) {
            address pool = factory.getPoolAddress(path[i], path[i + 1]);
            require(pool != address(0), "Router: Pool not found");
            
            (amounts[i + 1], ) = SimpleAMM(pool).getAmountOut(path[i], amounts[i]);
        }
    }
}