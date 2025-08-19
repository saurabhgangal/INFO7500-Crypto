// contracts/core/SimpleAMM.sol
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

/**
 * @title SimpleAMM
 * @author [Your Name]
 * @notice A constant product AMM with enhanced safety features
 * @dev Implements x*y=k formula with 0.3% swap fee
 */
contract SimpleAMM is ERC20, ReentrancyGuard, Ownable {
    IERC20 public immutable token0;
    IERC20 public immutable token1;
    
    uint256 public reserve0;
    uint256 public reserve1;
    uint256 public blockTimestampLast;
    
    uint256 public price0CumulativeLast;
    uint256 public price1CumulativeLast;
    
    uint256 private constant MINIMUM_LIQUIDITY = 10**3;
    uint256 private constant FEE_DENOMINATOR = 1000;
    uint256 private constant SWAP_FEE = 3; // 0.3%
    
    // Enhanced events for better tracking
    event Swap(
        address indexed sender,
        address indexed recipient,
        uint256 amount0In,
        uint256 amount1In,
        uint256 amount0Out,
        uint256 amount1Out,
        uint256 fee0,
        uint256 fee1
    );
    
    event Mint(address indexed sender, uint256 amount0, uint256 amount1, uint256 liquidity);
    event Burn(address indexed sender, uint256 amount0, uint256 amount1, uint256 liquidity);
    event Sync(uint256 reserve0, uint256 reserve1);
    event PriceUpdate(uint256 price0Cumulative, uint256 price1Cumulative);
    
    modifier validAddress(address _addr) {
        require(_addr != address(0), "AMM: Zero address");
        _;
    }
    
    constructor(
        address _token0,
        address _token1
    ) 
        validAddress(_token0) 
        validAddress(_token1) 
        ERC20("AMM LP Token", "AMM-LP") 
    {
        require(_token0 != _token1, "AMM: Identical tokens");
        token0 = IERC20(_token0);
        token1 = IERC20(_token1);
    }
    
    /**
     * @notice Get current reserves and last block timestamp
     * @return _reserve0 Reserve of token0
     * @return _reserve1 Reserve of token1
     * @return _blockTimestampLast Last block timestamp
     */
    function getReserves() 
        public 
        view 
        returns (
            uint256 _reserve0, 
            uint256 _reserve1, 
            uint256 _blockTimestampLast
        ) 
    {
        _reserve0 = reserve0;
        _reserve1 = reserve1;
        _blockTimestampLast = blockTimestampLast;
    }
    
    /**
     * @notice Update price accumulators
     * @dev Called on mint, burn, swap to track TWAP
     */
    function _update(
        uint256 balance0, 
        uint256 balance1, 
        uint256 _reserve0, 
        uint256 _reserve1
    ) private {
        require(balance0 <= type(uint112).max && balance1 <= type(uint112).max, "AMM: Overflow");
        
        uint256 blockTimestamp = block.timestamp;
        unchecked {
            uint256 timeElapsed = blockTimestamp - blockTimestampLast;
            if (timeElapsed > 0 && _reserve0 != 0 && _reserve1 != 0) {
                price0CumulativeLast += _reserve1 * timeElapsed / _reserve0;
                price1CumulativeLast += _reserve0 * timeElapsed / _reserve1;
            }
        }
        
        reserve0 = balance0;
        reserve1 = balance1;
        blockTimestampLast = blockTimestamp;
        
        emit Sync(reserve0, reserve1);
        emit PriceUpdate(price0CumulativeLast, price1CumulativeLast);
    }
    
    /**
     * @notice Add liquidity to the pool
     * @param amount0Desired Amount of token0 to add
     * @param amount1Desired Amount of token1 to add
     * @param amount0Min Minimum amount of token0 to add
     * @param amount1Min Minimum amount of token1 to add
     * @param to Recipient of LP tokens
     * @param deadline Transaction deadline
     * @return amount0 Actual amount of token0 added
     * @return amount1 Actual amount of token1 added
     * @return liquidity Amount of LP tokens minted
     */
    function addLiquidity(
        uint256 amount0Desired,
        uint256 amount1Desired,
        uint256 amount0Min,
        uint256 amount1Min,
        address to,
        uint256 deadline
    ) 
        external 
        nonReentrant 
        returns (
            uint256 amount0, 
            uint256 amount1, 
            uint256 liquidity
        ) 
    {
        require(block.timestamp <= deadline, "AMM: Expired");
        require(to != address(0), "AMM: Zero address");
        require(amount0Desired > 0 && amount1Desired > 0, "AMM: Amounts must be > 0");
        
        (amount0, amount1) = _calculateLiquidityAmounts(
            amount0Desired,
            amount1Desired,
            amount0Min,
            amount1Min
        );
        
        // Transfer tokens
        token0.transferFrom(msg.sender, address(this), amount0);
        token1.transferFrom(msg.sender, address(this), amount1);
        
        // Calculate liquidity
        uint256 _totalSupply = totalSupply();
        if (_totalSupply == 0) {
            // First liquidity provider
            uint256 sqrtK = _sqrt(amount0 * amount1);
            require(sqrtK > MINIMUM_LIQUIDITY, "AMM: Insufficient initial liquidity");
            liquidity = sqrtK - MINIMUM_LIQUIDITY;
            _mint(address(0), MINIMUM_LIQUIDITY); // Lock minimum liquidity
        } else {
            // Subsequent liquidity providers
            liquidity = _min(
                amount0 * _totalSupply / reserve0,
                amount1 * _totalSupply / reserve1
            );
            require(liquidity > 0, "AMM: Insufficient liquidity");
        }
        
        _mint(to, liquidity);
        
        // Update reserves
        uint256 balance0 = token0.balanceOf(address(this));
        uint256 balance1 = token1.balanceOf(address(this));
        _update(balance0, balance1, reserve0, reserve1);
        
        emit Mint(msg.sender, amount0, amount1, liquidity);
    }
    
    /**
     * @notice Remove liquidity from the pool
     * @param liquidity Amount of LP tokens to burn
     * @param amount0Min Minimum amount of token0 to receive
     * @param amount1Min Minimum amount of token1 to receive
     * @param to Recipient of tokens
     * @param deadline Transaction deadline
     * @return amount0 Amount of token0 received
     * @return amount1 Amount of token1 received
     */
    function removeLiquidity(
        uint256 liquidity,
        uint256 amount0Min,
        uint256 amount1Min,
        address to,
        uint256 deadline
    ) 
        external 
        nonReentrant 
        returns (uint256 amount0, uint256 amount1) 
    {
        require(block.timestamp <= deadline, "AMM: Expired");
        require(to != address(0), "AMM: Zero address");
        require(liquidity > 0, "AMM: Zero liquidity");
        
        uint256 _totalSupply = totalSupply();
        amount0 = liquidity * reserve0 / _totalSupply;
        amount1 = liquidity * reserve1 / _totalSupply;
        
        require(amount0 >= amount0Min, "AMM: Insufficient token0");
        require(amount1 >= amount1Min, "AMM: Insufficient token1");
        
        // Burn LP tokens
        _burn(msg.sender, liquidity);
        
        // Transfer tokens
        token0.transfer(to, amount0);
        token1.transfer(to, amount1);
        
        // Update reserves
        uint256 balance0 = token0.balanceOf(address(this));
        uint256 balance1 = token1.balanceOf(address(this));
        _update(balance0, balance1, reserve0, reserve1);
        
        emit Burn(msg.sender, amount0, amount1, liquidity);
    }
    
    /**
     * @notice Swap tokens
     * @param tokenIn Address of input token
     * @param amountIn Amount of input token
     * @param amountOutMin Minimum amount of output token
     * @param to Recipient address
     * @param deadline Transaction deadline
     * @return amountOut Amount of output token
     */
    function swap(
        address tokenIn,
        uint256 amountIn,
        uint256 amountOutMin,
        address to,
        uint256 deadline
    ) 
        external 
        nonReentrant 
        returns (uint256 amountOut) 
    {
        require(block.timestamp <= deadline, "AMM: Expired");
        require(to != address(0), "AMM: Zero address");
        require(tokenIn == address(token0) || tokenIn == address(token1), "AMM: Invalid token");
        require(amountIn > 0, "AMM: Zero input");
        require(reserve0 > 0 && reserve1 > 0, "AMM: Insufficient liquidity");
        
        bool isToken0 = tokenIn == address(token0);
        (
            IERC20 tokenInContract, 
            IERC20 tokenOutContract, 
            uint256 reserveIn, 
            uint256 reserveOut
        ) = isToken0 
            ? (token0, token1, reserve0, reserve1) 
            : (token1, token0, reserve1, reserve0);
        
        // Transfer input tokens
        tokenInContract.transferFrom(msg.sender, address(this), amountIn);
        
        // Calculate output amount with fee
        uint256 amountInWithFee = amountIn * (FEE_DENOMINATOR - SWAP_FEE);
        amountOut = (amountInWithFee * reserveOut) / 
                    (reserveIn * FEE_DENOMINATOR + amountInWithFee);
        
        require(amountOut >= amountOutMin, "AMM: Insufficient output");
        require(amountOut > 0, "AMM: Zero output");
        
        // Transfer output tokens
        tokenOutContract.transfer(to, amountOut);
        
        // Update reserves
        uint256 balance0 = token0.balanceOf(address(this));
        uint256 balance1 = token1.balanceOf(address(this));
        _update(balance0, balance1, reserve0, reserve1);
        
        // Calculate fee for event
        uint256 fee = amountIn * SWAP_FEE / FEE_DENOMINATOR;
        
        emit Swap(
            msg.sender,
            to,
            isToken0 ? amountIn : 0,
            isToken0 ? 0 : amountIn,
            isToken0 ? 0 : amountOut,
            isToken0 ? amountOut : 0,
            isToken0 ? fee : 0,
            isToken0 ? 0 : fee
        );
    }
    
    /**
     * @notice Get output amount for a given input
     * @param tokenIn Address of input token
     * @param amountIn Amount of input token
     * @return amountOut Expected output amount
     * @return priceImpact Price impact percentage (basis points)
     */
    function getAmountOut(
        address tokenIn, 
        uint256 amountIn
    ) 
        external 
        view 
        returns (uint256 amountOut, uint256 priceImpact) 
    {
        require(tokenIn == address(token0) || tokenIn == address(token1), "AMM: Invalid token");
        require(amountIn > 0, "AMM: Zero input");
        require(reserve0 > 0 && reserve1 > 0, "AMM: Insufficient liquidity");
        
        bool isToken0 = tokenIn == address(token0);
        (uint256 reserveIn, uint256 reserveOut) = isToken0 
            ? (reserve0, reserve1) 
            : (reserve1, reserve0);
        
        uint256 amountInWithFee = amountIn * (FEE_DENOMINATOR - SWAP_FEE);
        amountOut = (amountInWithFee * reserveOut) / 
                    (reserveIn * FEE_DENOMINATOR + amountInWithFee);
        
        // Calculate price impact in basis points (1 bp = 0.01%)
        uint256 exactQuote = amountIn * reserveOut / reserveIn;
        priceImpact = exactQuote > 0 ? ((exactQuote - amountOut) * 10000) / exactQuote : 0;
    }
    
    /**
     * @notice Calculate optimal liquidity amounts
     */
    function _calculateLiquidityAmounts(
        uint256 amount0Desired,
        uint256 amount1Desired,
        uint256 amount0Min,
        uint256 amount1Min
    ) 
        private 
        view 
        returns (uint256 amount0, uint256 amount1) 
    {
        if (reserve0 == 0 && reserve1 == 0) {
            // First liquidity provider
            (amount0, amount1) = (amount0Desired, amount1Desired);
        } else {
            // Subsequent liquidity providers
            uint256 amount1Optimal = amount0Desired * reserve1 / reserve0;
            if (amount1Optimal <= amount1Desired) {
                require(amount1Optimal >= amount1Min, "AMM: Insufficient amount1");
                (amount0, amount1) = (amount0Desired, amount1Optimal);
            } else {
                uint256 amount0Optimal = amount1Desired * reserve0 / reserve1;
                require(amount0Optimal <= amount0Desired, "AMM: Insufficient amount0");
                require(amount0Optimal >= amount0Min, "AMM: Insufficient amount0");
                (amount0, amount1) = (amount0Optimal, amount1Desired);
            }
        }
    }
    
    // Helper functions
    function _sqrt(uint256 y) private pure returns (uint256 z) {
        if (y > 3) {
            z = y;
            uint256 x = y / 2 + 1;
            while (x < z) {
                z = x;
                x = (y / x + x) / 2;
            }
        } else if (y != 0) {
            z = 1;
        } else {
            z = 0;
        }
    }
    
    function _min(uint256 x, uint256 y) private pure returns (uint256) {
        return x < y ? x : y;
    }
}