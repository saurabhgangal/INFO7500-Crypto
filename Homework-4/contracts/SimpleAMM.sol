// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/math/Math.sol";

/**
 * @title SimpleAMM
 * @dev A simplified automated market maker based on Uniswap V2's constant product formula
 */
contract SimpleAMM is ERC20, ReentrancyGuard {
    using Math for uint256;

    IERC20 public immutable tokenA;
    IERC20 public immutable tokenB;
    
    uint256 public reserveA;
    uint256 public reserveB;
    
    uint256 private constant MINIMUM_LIQUIDITY = 1000;
    
    event Deposit(address indexed provider, uint256 amountA, uint256 amountB, uint256 liquidity);
    event Redeem(address indexed provider, uint256 amountA, uint256 amountB, uint256 liquidity);
    event Swap(address indexed user, address indexed tokenIn, uint256 amountIn, uint256 amountOut);
    event Sync(uint256 reserveA, uint256 reserveB);
    
    constructor(address _tokenA, address _tokenB) ERC20("SimpleAMM LP Token", "SAMM-LP") {
        require(_tokenA != address(0), "Invalid token A address");
        require(_tokenB != address(0), "Invalid token B address");
        require(_tokenA != _tokenB, "Identical addresses");
        
        tokenA = IERC20(_tokenA);
        tokenB = IERC20(_tokenB);
    }
    
    /**
     * @dev Deposit tokens to provide liquidity
     * @param amountA Amount of token A to deposit
     * @param amountB Amount of token B to deposit
     * @return liquidity Amount of LP tokens minted
     */
    function deposit(uint256 amountA, uint256 amountB) external nonReentrant returns (uint256 liquidity) {
        require(amountA > 0 && amountB > 0, "Insufficient input amounts");
        
        // Transfer tokens from user
        tokenA.transferFrom(msg.sender, address(this), amountA);
        tokenB.transferFrom(msg.sender, address(this), amountB);
        
        uint256 _totalSupply = totalSupply();
        
        if (_totalSupply == 0) {
            // First deposit - mint liquidity equal to geometric mean minus minimum liquidity
            uint256 sqrtProduct = Math.sqrt(amountA * amountB);
            require(sqrtProduct > MINIMUM_LIQUIDITY, "Insufficient liquidity minted");
            liquidity = sqrtProduct - MINIMUM_LIQUIDITY;
            
            // Permanently lock the first MINIMUM_LIQUIDITY tokens
            _mint(address(1), MINIMUM_LIQUIDITY);
        } else {
            // Subsequent deposits - mint proportional to existing liquidity
            uint256 liquidityA = (amountA * _totalSupply) / reserveA;
            uint256 liquidityB = (amountB * _totalSupply) / reserveB;
            liquidity = Math.min(liquidityA, liquidityB);
            
            require(liquidity > 0, "Insufficient liquidity minted");
        }
        
        // Mint LP tokens to provider
        _mint(msg.sender, liquidity);
        
        // Update reserves
        _update();
        
        emit Deposit(msg.sender, amountA, amountB, liquidity);
    }
    
    /**
     * @dev Redeem LP tokens to withdraw liquidity
     * @param liquidity Amount of LP tokens to burn
     * @return amountA Amount of token A returned
     * @return amountB Amount of token B returned
     */
    function redeem(uint256 liquidity) external nonReentrant returns (uint256 amountA, uint256 amountB) {
        require(liquidity > 0, "Insufficient liquidity burned");
        require(balanceOf(msg.sender) >= liquidity, "Insufficient balance");
        
        uint256 _totalSupply = totalSupply();
        
        // Calculate token amounts proportional to liquidity share
        amountA = (liquidity * reserveA) / _totalSupply;
        amountB = (liquidity * reserveB) / _totalSupply;
        
        require(amountA > 0 && amountB > 0, "Insufficient liquidity burned");
        
        // Burn LP tokens
        _burn(msg.sender, liquidity);
        
        // Transfer tokens to user
        tokenA.transfer(msg.sender, amountA);
        tokenB.transfer(msg.sender, amountB);
        
        // Update reserves
        _update();
        
        emit Redeem(msg.sender, amountA, amountB, liquidity);
    }
    
    /**
     * @dev Swap tokens using constant product formula
     * @param tokenIn Address of input token
     * @param amountIn Amount of input token
     * @param minAmountOut Minimum amount of output token expected
     * @return amountOut Amount of output token received
     */
    function swap(address tokenIn, uint256 amountIn, uint256 minAmountOut) external nonReentrant returns (uint256 amountOut) {
        require(amountIn > 0, "Insufficient input amount");
        require(tokenIn == address(tokenA) || tokenIn == address(tokenB), "Invalid token");
        
        bool isTokenA = tokenIn == address(tokenA);
        
        // Get reserves
        (uint256 reserveIn, uint256 reserveOut) = isTokenA ? (reserveA, reserveB) : (reserveB, reserveA);
        
        // Transfer input token
        IERC20(tokenIn).transferFrom(msg.sender, address(this), amountIn);
        
        // Calculate output amount with 0.3% fee
        uint256 amountInWithFee = amountIn * 997;
        amountOut = (amountInWithFee * reserveOut) / (reserveIn * 1000 + amountInWithFee);
        
        require(amountOut >= minAmountOut, "Insufficient output amount");
        require(amountOut > 0, "Insufficient liquidity");
        
        // Transfer output token
        if (isTokenA) {
            tokenB.transfer(msg.sender, amountOut);
        } else {
            tokenA.transfer(msg.sender, amountOut);
        }
        
        // Update reserves
        _update();
        
        emit Swap(msg.sender, tokenIn, amountIn, amountOut);
    }
    
    /**
     * @dev Get output amount for a given input amount
     * @param amountIn Amount of input token
     * @param reserveIn Reserve of input token
     * @param reserveOut Reserve of output token
     * @return amountOut Amount of output token
     */
    function getAmountOut(uint256 amountIn, uint256 reserveIn, uint256 reserveOut) public pure returns (uint256 amountOut) {
        require(amountIn > 0, "Insufficient input amount");
        require(reserveIn > 0 && reserveOut > 0, "Insufficient liquidity");
        
        uint256 amountInWithFee = amountIn * 997;
        uint256 numerator = amountInWithFee * reserveOut;
        uint256 denominator = reserveIn * 1000 + amountInWithFee;
        amountOut = numerator / denominator;
    }
    
    /**
     * @dev Get input amount required for a desired output amount
     * @param amountOut Desired amount of output token
     * @param reserveIn Reserve of input token
     * @param reserveOut Reserve of output token
     * @return amountIn Amount of input token required
     */
    function getAmountIn(uint256 amountOut, uint256 reserveIn, uint256 reserveOut) public pure returns (uint256 amountIn) {
        require(amountOut > 0, "Insufficient output amount");
        require(reserveIn > 0 && reserveOut > 0, "Insufficient liquidity");
        require(amountOut < reserveOut, "Insufficient liquidity");
        
        uint256 numerator = reserveIn * amountOut * 1000;
        uint256 denominator = (reserveOut - amountOut) * 997;
        amountIn = (numerator / denominator) + 1;
    }
    
    /**
     * @dev Update reserves based on current balances
     */
    function _update() private {
        reserveA = tokenA.balanceOf(address(this));
        reserveB = tokenB.balanceOf(address(this));
        emit Sync(reserveA, reserveB);
    }
    
    /**
     * @dev Get reserves
     * @return _reserveA Reserve of token A
     * @return _reserveB Reserve of token B
     */
    function getReserves() external view returns (uint256 _reserveA, uint256 _reserveB) {
        _reserveA = reserveA;
        _reserveB = reserveB;
    }
}