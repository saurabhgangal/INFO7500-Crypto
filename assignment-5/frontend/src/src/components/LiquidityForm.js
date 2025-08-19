// frontend/src/components/LiquidityForm.js
import React, { useState, useEffect } from 'react';
import { useAccount, useContractRead, useContractWrite, useWaitForTransaction } from 'wagmi';
import { parseEther, formatEther } from 'viem';
import toast from 'react-hot-toast';
import { motion } from 'framer-motion';
import ammABI from '../abi/SimpleAMM.json';
import erc20ABI from '../abi/ERC20.json';
import TokenBalance from './TokenBalance';
import LiquidityCalculator from './LiquidityCalculator';
import { saveTransaction } from '../utils/storage';
import { formatNumber } from '../utils/format';

function LiquidityForm({ pool }) {
  const { address, isConnected } = useAccount();
  const [activeTab, setActiveTab] = useState('add');
  const [amount0, setAmount0] = useState('');
  const [amount1, setAmount1] = useState('');
  const [lpAmount, setLpAmount] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [showCalculator, setShowCalculator] = useState(false);
  const [expectedLp, setExpectedLp] = useState('0');
  const [shareOfPool, setShareOfPool] = useState('0');

  // Get user's LP balance
  const { data: lpBalance } = useContractRead({
    address: pool.address,
    abi: ammABI,
    functionName: 'balanceOf',
    args: [address],
    enabled: !!address,
    watch: true,
  });

  // Get total supply
  const { data: totalSupply } = useContractRead({
    address: pool.address,
    abi: ammABI,
    functionName: 'totalSupply',
    watch: true,
  });

  // Calculate expected LP tokens and pool share
  useEffect(() => {
    if (amount0 && amount1 && pool.reserve0 && pool.reserve1 && totalSupply) {
      const amount0Wei = parseEther(amount0);
      const amount1Wei = parseEther(amount1);
      
      if (totalSupply === 0n) {
        // First deposit
        const lpTokens = sqrt(amount0Wei * amount1Wei);
        setExpectedLp(formatEther(lpTokens));
        setShareOfPool('100');
      } else {
        // Calculate based on existing ratio
        const lpFromToken0 = (amount0Wei * totalSupply) / pool.reserve0;
        const lpFromToken1 = (amount1Wei * totalSupply) / pool.reserve1;
        const lpTokens = lpFromToken0 < lpFromToken1 ? lpFromToken0 : lpFromToken1;
        
        setExpectedLp(formatEther(lpTokens));
        const newShare = (lpTokens * 10000n) / (totalSupply + lpTokens);
        setShareOfPool((Number(newShare) / 100).toFixed(2));
      }
    }
  }, [amount0, amount1, pool, totalSupply]);

  // Auto-calculate amount1 based on amount0
  useEffect(() => {
    if (amount0 && pool.reserve0 && pool.reserve1 && activeTab === 'add') {
      const amount0Wei = parseEther(amount0);
      const amount1Wei = (amount0Wei * pool.reserve1) / pool.reserve0;
      setAmount1(formatEther(amount1Wei));
    }
  }, [amount0, pool, activeTab]);

  // Calculate amounts when removing liquidity
  useEffect(() => {
    if (lpAmount && totalSupply && pool.reserve0 && pool.reserve1 && activeTab === 'remove') {
      const lpWei = parseEther(lpAmount);
      const amount0Out = (lpWei * pool.reserve0) / totalSupply;
      const amount1Out = (lpWei * pool.reserve1) / totalSupply;
      
      setAmount0(formatEther(amount0Out));
      setAmount1(formatEther(amount1Out));
    }
  }, [lpAmount, totalSupply, pool, activeTab]);

  // Contract writes
  const { write: addLiquidity, data: addData } = useContractWrite({
    address: pool.address,
    abi: ammABI,
    functionName: 'addLiquidity',
  });

  const { write: removeLiquidity, data: removeData } = useContractWrite({
    address: pool.address,
    abi: ammABI,
    functionName: 'removeLiquidity',
  });

  // Approval writes
  const { write: approveToken0 } = useContractWrite({
    address: pool.token0,
    abi: erc20ABI,
    functionName: 'approve',
  });

  const { write: approveToken1 } = useContractWrite({
    address: pool.token1,
    abi: erc20ABI,
    functionName: 'approve',
  });

  // Transaction confirmations
  const { isLoading: isAdding } = useWaitForTransaction({
    hash: addData?.hash,
    onSuccess(data) {
      toast.success('Liquidity added successfully!');
      saveTransaction({
        type: 'add',
        hash: data.transactionHash,
        token0: pool.symbol0,
        token1: pool.symbol1,
        amount0,
        amount1,
        lpReceived: expectedLp,
        timestamp: Date.now(),
      });
      setAmount0('');
      setAmount1('');
      setIsProcessing(false);
    },
    onError() {
      toast.error('Failed to add liquidity');
      setIsProcessing(false);
    },
  });

  const { isLoading: isRemoving } = useWaitForTransaction({
    hash: removeData?.hash,
    onSuccess(data) {
      toast.success('Liquidity removed successfully!');
      saveTransaction({
        type: 'remove',
        hash: data.transactionHash,
        token0: pool.symbol0,
        token1: pool.symbol1,
        amount0,
        amount1,
        lpAmount,
        timestamp: Date.now(),
      });
      setLpAmount('');
      setAmount0('');
      setAmount1('');
      setIsProcessing(false);
    },
    onError() {
      toast.error('Failed to remove liquidity');
      setIsProcessing(false);
    },
  });

  const handleAdd = async () => {
    if (!amount0 || !amount1) {
      toast.error('Please enter amounts');
      return;
    }

    setIsProcessing(true);
    
    try {
      // Approve both tokens
      await approveToken0({
        args: [pool.address, parseEther(amount0)],
      });
      
      await approveToken1({
        args: [pool.address, parseEther(amount1)],
      });
      
      // Add liquidity
      const deadline = Math.floor(Date.now() / 1000) + 1200;
      await addLiquidity({
        args: [
          parseEther(amount0),
          parseEther(amount1),
          0, // Accept any amount of tokens (for demo)
          0,
          address,
          deadline,
        ],
      });
    } catch (error) {
      toast.error(error.message || 'Transaction failed');
      setIsProcessing(false);
    }
  };

  const handleRemove = async () => {
    if (!lpAmount || Number(lpAmount) <= 0) {
      toast.error('Please enter LP amount');
      return;
    }

    if (parseEther(lpAmount) > lpBalance) {
      toast.error('Insufficient LP balance');
      return;
    }

    setIsProcessing(true);
    
    try {
      const deadline = Math.floor(Date.now() / 1000) + 1200;
      await removeLiquidity({
        args: [
          parseEther(lpAmount),
          0, // Accept any amount of tokens (for demo)
          0,
          address,
          deadline,
        ],
      });
    } catch (error) {
      toast.error(error.message || 'Transaction failed');
      setIsProcessing(false);
    }
  };

  const sqrt = (value) => {
    if (value === 0n) return 0n;
    let z = value;
    let x = value / 2n + 1n;
    while (x < z) {
      z = x;
      x = (value / x + x) / 2n;
    }
    return z;
  };

  return (
    <motion.div 
      className="liquidity-form card"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      <div className="card-header">
        <h2>Liquidity</h2>
        <button 
          className="calculator-btn"
          onClick={() => setShowCalculator(!showCalculator)}
        >
          🧮
        </button>
      </div>
      
      {showCalculator && (
        <LiquidityCalculator 
          pool={pool}
          onClose={() => setShowCalculator(false)}
        />
      )}
      
      <div className="tabs">
        <button
          className={activeTab === 'add' ? 'active' : ''}
          onClick={() => setActiveTab('add')}
        >
          Add Liquidity
        </button>
        <button
          className={activeTab === 'remove' ? 'active' : ''}
          onClick={() => setActiveTab('remove')}
        >
          Remove Liquidity
        </button>
      </div>

      {activeTab === 'add' ? (
        <div className="add-liquidity">
          <div className="form-group">
            <div className="input-header">
              <label>{pool.symbol0} Amount</label>
              <TokenBalance tokenAddress={pool.token0} symbol={pool.symbol0} />
            </div>
            <input
              type="number"
              placeholder="0.0"
              value={amount0}
              onChange={(e) => setAmount0(e.target.value)}
              disabled={!isConnected || isProcessing}
            />
          </div>

          <div className="plus-icon">+</div>

          <div className="form-group">
            <div className="input-header">
              <label>{pool.symbol1} Amount</label>
              <TokenBalance tokenAddress={pool.token1} symbol={pool.symbol1} />
            </div>
            <input
              type="number"
              placeholder="0.0"
              value={amount1}
              onChange={(e) => setAmount1(e.target.value)}
              disabled={!isConnected || isProcessing}
            />
          </div>

          {amount0 && amount1 && (
            <motion.div 
              className="liquidity-preview"
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
            >
              <div className="preview-row">
                <span>Expected LP Tokens</span>
                <span>{formatNumber(expectedLp)}</span>
              </div>
              <div className="preview-row">
                <span>Share of Pool</span>
                <span>{shareOfPool}%</span>
              </div>
              <div className="preview-row">
                <span>Price Impact</span>
                <span className="positive">{'<'}0.01%</span>
              </div>
            </motion.div>
          )}

          <motion.button
            onClick={handleAdd}
            disabled={!isConnected || !amount0 || !amount1 || isProcessing}
            className="btn-primary"
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            {isProcessing ? 'Processing...' : 'Add Liquidity'}
          </motion.button>
        </div>
      ) : (
        <div className="remove-liquidity">
          <div className="lp-balance-display">
            <span>Your LP Balance</span>
            <span className="balance-value">
              {lpBalance ? formatEther(lpBalance) : '0'} LP
            </span>
          </div>

          <div className="form-group">
            <label>LP Tokens to Remove</label>
            <div className="input-with-max">
              <input
                type="number"
                placeholder="0.0"
                value={lpAmount}
                onChange={(e) => setLpAmount(e.target.value)}
                disabled={!isConnected || isProcessing}
              />
              <button 
                className="max-btn"
                onClick={() => setLpAmount(lpBalance ? formatEther(lpBalance) : '0')}
              >
                MAX
              </button>
            </div>
            
            <input 
              type="range"
              min="0"
              max="100"
              value={lpBalance && lpAmount ? 
                (Number(lpAmount) / Number(formatEther(lpBalance))) * 100 : 0
              }
              onChange={(e) => {
                if (lpBalance) {
                  const percent = Number(e.target.value) / 100;
                  setLpAmount(formatEther(lpBalance * BigInt(Math.floor(percent * 1000)) / 1000n));
                }
              }}
              className="percentage-slider"
            />
          </div>

          {lpAmount && Number(lpAmount) > 0 && (
            <motion.div 
              className="removal-preview"
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
            >
              <h4>You will receive:</h4>
              <div className="token-amount">
                <span>{pool.symbol0}</span>
                <span>{formatNumber(amount0)}</span>
              </div>
              <div className="token-amount">
                <span>{pool.symbol1}</span>
                <span>{formatNumber(amount1)}</span>
              </div>
            </motion.div>
          )}

          <motion.button
            onClick={handleRemove}
            disabled={!isConnected || !lpAmount || isProcessing}
            className="btn-primary remove-btn"
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            {isProcessing ? 'Processing...' : 'Remove Liquidity'}
          </motion.button>
        </div>
      )}
    </motion.div>
  );
}

export default LiquidityForm;