// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";

interface ISimpleAMM {
    function deposit(uint256 amountA, uint256 amountB) external returns (uint256);
    function redeem(uint256 liquidity) external returns (uint256, uint256);
    function swap(address tokenIn, uint256 amountIn, uint256 minAmountOut) external returns (uint256);
    function balanceOf(address account) external view returns (uint256);
}

contract MaliciousReentrant is IERC20 {
    ISimpleAMM public amm;
    bool public attacking;
    address public tokenA;
    address public tokenB;
    
    constructor(address _amm) {
        amm = ISimpleAMM(_amm);
    }
    
    function deposit(address tokenA, address tokenB, uint256 amountA, uint256 amountB) external {
        IERC20(tokenA).approve(address(amm), type(uint256).max);
        IERC20(tokenB).approve(address(amm), type(uint256).max);
        amm.deposit(amountA, amountB);
    }
    
    function attackDeposit(address _tokenA, address _tokenB) external {
        tokenA = _tokenA;
        tokenB = _tokenB;
        IERC20(tokenA).approve(address(amm), type(uint256).max);
        IERC20(tokenB).approve(address(amm), type(uint256).max);
        attacking = true;
        amm.deposit(100e18, 200e18);
    }
    
    function attackRedeem() external {
        attacking = true;
        uint256 balance = amm.balanceOf(address(this));
        amm.redeem(balance);
    }
    
    function attackSwap(address tokenIn, uint256 amountIn) external {
        IERC20(tokenIn).approve(address(amm), type(uint256).max);
        attacking = true;
        amm.swap(tokenIn, amountIn, 0);
    }
    
    // ERC20 functions to make this contract act as a malicious token
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;
    uint256 public totalSupply;
    uint8 public decimals = 18;
    string public name = "Malicious";
    string public symbol = "MAL";
    
    function transfer(address to, uint256 amount) external returns (bool) {
        // During transfer, attempt reentrancy
        if (attacking && to == address(amm)) {
            attacking = false;
            try amm.deposit(1e18, 2e18) {} catch {}
        }
        
        require(balanceOf[msg.sender] >= amount, "Insufficient balance");
        balanceOf[msg.sender] -= amount;
        balanceOf[to] += amount;
        emit Transfer(msg.sender, to, amount);
        return true;
    }
    
    function transferFrom(address from, address to, uint256 amount) external returns (bool) {
        // During transferFrom, attempt reentrancy
        if (attacking && to == address(amm)) {
            attacking = false;
            try amm.deposit(1e18, 2e18) {} catch {}
            try amm.redeem(1) {} catch {}
            try amm.swap(tokenA, 1e18, 0) {} catch {}
        }
        
        require(allowance[from][msg.sender] >= amount, "Insufficient allowance");
        require(balanceOf[from] >= amount, "Insufficient balance");
        allowance[from][msg.sender] -= amount;
        balanceOf[from] -= amount;
        balanceOf[to] += amount;
        emit Transfer(from, to, amount);
        return true;
    }
    
    function approve(address spender, uint256 amount) external returns (bool) {
        allowance[msg.sender][spender] = amount;
        emit Approval(msg.sender, spender, amount);
        return true;
    }
    
    // Give this contract some balance for testing
    function mint(uint256 amount) external {
        balanceOf[address(this)] += amount;
        totalSupply += amount;
        emit Transfer(address(0), address(this), amount);
    }
}