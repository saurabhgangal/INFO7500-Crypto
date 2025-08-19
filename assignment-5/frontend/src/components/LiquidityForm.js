import React, { useState, useEffect } from 'react';
import { useContractRead, useContractWrite, useWaitForTransaction, useAccount } from 'wagmi';
import { parseEther, formatEther } from 'viem';
import toast from 'react-hot-toast';
import ammABI from '../abi/SimpleAMM.json';
import { getContractAddress } from '../utils/contracts';

function LiquidityForm({ pool }) {
  const [activeTab, setActiveTab] = useState('add');
  const [amount0, setAmount0] = useState('');
  const [amount1, setAmount1] = useState('');
  const [lpAmount, setLpAmount] = useState('');
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

  // Read user's LP token balance
  const { data: userLpBalance, isLoading: lpBalanceLoading } = useContractRead({
    address: POOL_ADDRESS,
    abi: ammABI,
    functionName: 'balanceOf',
    args: [address],
    enabled: !!address,
    watch: true,
  });

  // Read total supply for calculations
  const { data: totalSupply, isLoading: totalSupplyLoading } = useContractRead({
    address: POOL_ADDRESS,
    abi: ammABI,
    functionName: 'totalSupply',
    watch: true,
  });

  // Add liquidity transaction
  const { write: addLiquidity, data: addLiquidityData } = useContractWrite({
    address: POOL_ADDRESS,
    abi: ammABI,
    functionName: 'addLiquidity',
  });

  // Remove liquidity transaction
  const { write: removeLiquidity, data: removeLiquidityData } = useContractWrite({
    address: POOL_ADDRESS,
    abi: ammABI,
    functionName: 'removeLiquidity',
  });

  // Wait for add liquidity transaction
  const { isLoading: isAddingLiquidity, isSuccess: isAddSuccess } = useWaitForTransaction({
    hash: addLiquidityData?.hash,
    onSuccess: () => {
      toast.success('Liquidity added successfully!');
      setAmount0('');
      setAmount1('');
    },
    onError: (error) => {
      toast.error(`Failed to add liquidity: ${error.message}`);
    },
  });

  // Wait for remove liquidity transaction
  const { isLoading: isRemovingLiquidity, isSuccess: isRemoveSuccess } = useWaitForTransaction({
    hash: removeLiquidityData?.hash,
    onSuccess: () => {
      toast.success('Liquidity removed successfully!');
      setLpAmount('');
    },
    onError: (error) => {
      toast.error(`Failed to remove liquidity: ${error.message}`);
    },
  });

  // Auto-calculate amount1 based on amount0 for adding liquidity
  const handleAmount0Change = (value) => {
    setAmount0(value);
    if (value && reserves && reserves[0] > 0 && reserves[1] > 0) {
      const ratio = Number(reserves[1]) / Number(reserves[0]);
      setAmount1((parseFloat(value) * ratio).toFixed(6));
    }
  };

  const handleAddLiquidity = async () => {
    if (!address) {
      toast.error('Please connect your wallet first');
      return;
    }

    if (!amount0 || !amount1) {
      toast.error('Please enter both amounts');
      return;
    }

    try {
      addLiquidity({
        args: [
          parseEther(amount0),
          parseEther(amount1),
          0, // amount0Min
          0, // amount1Min
          address,
          Math.floor(Date.now() / 1000) + 3600 // 1 hour deadline
        ],
      });
    } catch (error) {
      toast.error(`Error adding liquidity: ${error.message}`);
    }
  };

  const handleRemoveLiquidity = async () => {
    if (!address) {
      toast.error('Please connect your wallet first');
      return;
    }

    if (!lpAmount || parseFloat(lpAmount) <= 0) {
      toast.error('Please enter LP token amount');
      return;
    }

    try {
      removeLiquidity({
        args: [
          parseEther(lpAmount),
          0, // amount0Min
          0, // amount1Min
          address,
          Math.floor(Date.now() / 1000) + 3600 // 1 hour deadline
        ],
      });
    } catch (error) {
      toast.error(`Error removing liquidity: ${error.message}`);
    }
  };

  // Calculate pool share and LP tokens
  const calculatePoolShare = () => {
    if (!userLpBalance || !totalSupply || !amount0 || !reserves) return { share: '0%', lpTokens: '0' };
    
    const userLp = Number(formatEther(userLpBalance));
    const total = Number(formatEther(totalSupply));
    const share = total > 0 ? (userLp / total * 100).toFixed(2) : '0';
    
    // Estimate LP tokens for new liquidity
    const newLpTokens = reserves[0] > 0 ? 
      (parseFloat(amount0) * Number(formatEther(totalSupply)) / Number(formatEther(reserves[0]))) : 0;
    
    return { share: `${share}%`, lpTokens: newLpTokens.toFixed(2) };
  };

  // Calculate tokens to receive when removing liquidity
  const calculateRemovalAmounts = () => {
    if (!lpAmount || !reserves || !totalSupply) return { token0: '0', token1: '0' };
    
    const lpAmountNum = parseFloat(lpAmount);
    const total = Number(formatEther(totalSupply));
    const reserve0 = Number(formatEther(reserves[0]));
    const reserve1 = Number(formatEther(reserves[1]));
    
    const token0Amount = (lpAmountNum * reserve0 / total).toFixed(2);
    const token1Amount = (lpAmountNum * reserve1 / total).toFixed(2);
    
    return { token0: token0Amount, token1: token1Amount };
  };

  const { share, lpTokens } = calculatePoolShare();
  const { token0: removeToken0, token1: removeToken1 } = calculateRemovalAmounts();

  if (!pool) {
    return (
      <div className="liquidity-form">
        <div className="liquidity-header">
          <h3>Manage Liquidity</h3>
        </div>
        <p>Please select a pool to manage liquidity</p>
      </div>
    );
  }

  return (
    <div className="liquidity-form">
      <div className="liquidity-header">
        <h3>Manage Liquidity</h3>
      </div>

      <div className="liquidity-tabs">
        <button
          className={`tab ${activeTab === 'add' ? 'active' : ''}`}
          onClick={() => setActiveTab('add')}
        >
          Add Liquidity
        </button>
        <button
          className={`tab ${activeTab === 'remove' ? 'active' : ''}`}
          onClick={() => setActiveTab('remove')}
        >
          Remove Liquidity
        </button>
      </div>

      {activeTab === 'add' ? (
        <div className="add-liquidity">
          <div className="form-group">
            <div className="input-header">
              <label>{pool.symbol0 || 'Token0'} Amount</label>
              <span className="balance">Balance: 1000.00</span>
            </div>
            <input
              type="number"
              placeholder="0.0"
              value={amount0}
              onChange={(e) => handleAmount0Change(e.target.value)}
              disabled={isAddingLiquidity}
            />
          </div>

          <div className="plus-sign">+</div>

          <div className="form-group">
            <div className="input-header">
              <label>{pool.symbol1 || 'Token1'} Amount</label>
              <span className="balance">Balance: 500.00</span>
            </div>
            <input
              type="number"
              placeholder="0.0"
              value={amount1}
              onChange={(e) => setAmount1(e.target.value)}
              disabled={isAddingLiquidity}
            />
          </div>

          <div className="liquidity-info">
            <div className="info-row">
              <span>Pool Share</span>
              <span>{share}</span>
            </div>
            <div className="info-row">
              <span>LP Tokens</span>
              <span>~{lpTokens}</span>
            </div>
          </div>

          <button
            onClick={handleAddLiquidity}
            disabled={!amount0 || !amount1 || isAddingLiquidity || !address}
            className="btn-primary"
          >
            {isAddingLiquidity ? 'Adding Liquidity...' : 'Add Liquidity'}
          </button>
        </div>
      ) : (
        <div className="remove-liquidity">
          <div className="form-group">
            <div className="input-header">
              <label>LP Token Amount</label>
              <span className="balance">Balance: {userLpBalance ? formatEther(userLpBalance) : '0'} LP</span>
            </div>
            <input
              type="number"
              placeholder="0.0"
              value={lpAmount}
              onChange={(e) => setLpAmount(e.target.value)}
              disabled={isRemovingLiquidity}
            />
          </div>

          <div className="removal-info">
            <h4>You will receive:</h4>
            <div className="token-amount">
              <span>{pool.symbol0 || 'Token0'}</span>
              <span>~{removeToken0}</span>
            </div>
            <div className="token-amount">
              <span>{pool.symbol1 || 'Token1'}</span>
              <span>~{removeToken1}</span>
            </div>
          </div>

          <button
            onClick={handleRemoveLiquidity}
            disabled={!lpAmount || isRemovingLiquidity || !address}
            className="btn-primary"
          >
            {isRemovingLiquidity ? 'Removing Liquidity...' : 'Remove Liquidity'}
          </button>
        </div>
      )}
      
      {!address && (
        <p className="warning">⚠️ Please connect your wallet to manage liquidity</p>
      )}
    </div>
  );
}

export default LiquidityForm;
