import React, { useState, useEffect } from 'react';
import { useContractRead, useContractWrite, useWaitForTransaction, useAccount } from 'wagmi';
import { parseEther, formatEther } from 'viem';
import toast from 'react-hot-toast';
import ammABI from '../abi/SimpleAMM.json';
import { getContractAddress } from '../utils/contracts';

function SwapForm({ pool }) {
  const [tokenIn, setTokenIn] = useState('token0');
  const [amountIn, setAmountIn] = useState('');
  const [amountOut, setAmountOut] = useState('0');
  const [slippage, setSlippage] = useState(0.5);
  const { address } = useAccount();

  // Contract addresses - these should come from environment or be configurable
  const POOL_ADDRESS = pool?.address || getContractAddress('FACTORY');

  // Read pool reserves
  const { data: reserves, isLoading: reservesLoading } = useContractRead({
    address: POOL_ADDRESS,
    abi: ammABI,
    functionName: 'getReserves',
    watch: true,
  });

  // Get swap output amount
  const { data: swapOutput, isLoading: swapOutputLoading } = useContractRead({
    address: POOL_ADDRESS,
    abi: ammABI,
    functionName: 'getAmountOut',
    args: [
      tokenIn === 'token0' ? pool?.token0 || '0x0' : pool?.token1 || '0x0',
      amountIn ? parseEther(amountIn) : 0n
    ],
    enabled: !!amountIn && !!pool && parseFloat(amountIn) > 0,
    watch: true,
  });

  // Swap transaction
  const { write: swap, data: swapData } = useContractWrite({
    address: POOL_ADDRESS,
    abi: ammABI,
    functionName: 'swap',
  });

  // Wait for swap transaction
  const { isLoading: isSwapping, isSuccess: isSwapSuccess } = useWaitForTransaction({
    hash: swapData?.hash,
    onSuccess: () => {
      toast.success('Swap successful!');
      setAmountIn('');
      setAmountOut('0');
    },
    onError: (error) => {
      toast.error(`Swap failed: ${error.message}`);
    },
  });

  // Calculate output amount when input changes
  useEffect(() => {
    if (swapOutput && swapOutput[0]) {
      const outputAmount = formatEther(swapOutput[0]);
      setAmountOut(outputAmount);
    } else if (amountIn && reserves) {
      // Fallback calculation if getAmountOut fails
      const [reserve0, reserve1] = reserves;
      const rate = tokenIn === 'token0' ? 
        Number(reserve1) / Number(reserve0) : 
        Number(reserve0) / Number(reserve1);
      const output = parseFloat(amountIn) * rate * 0.997; // 0.3% fee
      setAmountOut(output.toFixed(6));
    } else {
      setAmountOut('0');
    }
  }, [amountIn, tokenIn, swapOutput, reserves]);

  const handleSwap = async () => {
    if (!address) {
      toast.error('Please connect your wallet first');
      return;
    }

    if (!amountIn || parseFloat(amountIn) <= 0) {
      toast.error('Please enter a valid amount');
      return;
    }

    if (!pool) {
      toast.error('No pool selected');
      return;
    }

    try {
      const tokenInAddress = tokenIn === 'token0' ? pool.token0 : pool.token1;
      const amountOutMin = parseFloat(amountOut) * (1 - slippage / 100);
      
      swap({
        args: [
          tokenInAddress,
          parseEther(amountIn),
          parseEther(amountOutMin.toString()),
          address,
          Math.floor(Date.now() / 1000) + 3600 // 1 hour deadline
        ],
      });
    } catch (error) {
      toast.error(`Error initiating swap: ${error.message}`);
    }
  };

  const flipTokens = () => {
    setTokenIn(tokenIn === 'token0' ? 'token1' : 'token0');
    setAmountIn('');
    setAmountOut('0');
  };

  if (!pool) {
    return (
      <div className="swap-form">
        <div className="swap-header">
          <h3>Swap Tokens</h3>
        </div>
        <p>Please select a pool to start swapping</p>
      </div>
    );
  }

  return (
    <div className="swap-form">
      <div className="swap-header">
        <h3>Swap Tokens</h3>
        <button className="settings-btn" title="Settings">⚙️</button>
      </div>

      <div className="swap-inputs">
        <div className="swap-input-group">
          <div className="input-header">
            <label>From</label>
            <span className="balance">Balance: 1000.00</span>
          </div>
          <div className="token-input">
            <select 
              value={tokenIn} 
              onChange={(e) => setTokenIn(e.target.value)}
              disabled={isSwapping}
            >
              <option value="token0">{pool.symbol0 || 'Token0'}</option>
              <option value="token1">{pool.symbol1 || 'Token1'}</option>
            </select>
            <input
              type="number"
              placeholder="0.0"
              value={amountIn}
              onChange={(e) => setAmountIn(e.target.value)}
              disabled={isSwapping}
            />
          </div>
        </div>

        <button className="flip-btn" onClick={flipTokens} disabled={isSwapping}>
          ⇅
        </button>

        <div className="swap-input-group">
          <div className="input-header">
            <label>To (estimated)</label>
            <span className="balance">Balance: 500.00</span>
          </div>
          <div className="token-input">
            <div className="token-display">
              {tokenIn === 'token0' ? (pool.symbol1 || 'Token1') : (pool.symbol0 || 'Token0')}
            </div>
            <input
              type="number"
              placeholder="0.0"
              value={amountOut}
              disabled
              readOnly
            />
          </div>
        </div>
      </div>

      <div className="swap-info">
        {reserves && (
          <div className="info-row">
            <span>Rate</span>
            <span>
              1 {tokenIn === 'token0' ? (pool.symbol0 || 'Token0') : (pool.symbol1 || 'Token1')} = 
              {tokenIn === 'token0' ? 
                (Number(reserves[1]) / Number(reserves[0])).toFixed(6) : 
                (Number(reserves[0]) / Number(reserves[1])).toFixed(6)
              } {tokenIn === 'token0' ? (pool.symbol1 || 'Token1') : (pool.symbol0 || 'Token0')}
            </span>
          </div>
        )}
        <div className="info-row">
          <span>Slippage Tolerance</span>
          <span>{slippage}%</span>
        </div>
        <div className="info-row">
          <span>Fee</span>
          <span>0.3%</span>
        </div>
      </div>

      <button
        onClick={handleSwap}
        disabled={!amountIn || parseFloat(amountIn) <= 0 || isSwapping || !address}
        className="btn-primary swap-btn"
      >
        {isSwapping ? 'Swapping...' : 'Swap'}
      </button>
      
      {!address && (
        <p className="warning">⚠️ Please connect your wallet to swap tokens</p>
      )}
    </div>
  );
}

export default SwapForm;