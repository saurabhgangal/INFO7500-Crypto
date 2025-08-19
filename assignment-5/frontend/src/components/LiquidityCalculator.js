// frontend/src/components/LiquidityCalculator.js
import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { formatEther, parseEther } from 'viem';
import { calculateLPTokens, calculatePoolShare } from '../utils/calculations';

function LiquidityCalculator({ pool, onClose }) {
  const [token0Amount, setToken0Amount] = useState('');
  const [token1Amount, setToken1Amount] = useState('');
  const [calculatedLp, setCalculatedLp] = useState('0');
  const [poolShare, setPoolShare] = useState('0');

  useEffect(() => {
    if (token0Amount && token1Amount && pool) {
      try {
        const amount0Wei = parseEther(token0Amount);
        const amount1Wei = parseEther(token1Amount);
        
        const lpTokens = calculateLPTokens(
          amount0Wei,
          amount1Wei,
          pool.reserve0,
          pool.reserve1,
          pool.totalSupply || 0n
        );
        
        setCalculatedLp(formatEther(lpTokens));
        
        const share = calculatePoolShare(lpTokens, pool.totalSupply || 0n);
        setPoolShare(share.toFixed(2));
      } catch (error) {
        console.error('Calculation error:', error);
      }
    }
  }, [token0Amount, token1Amount, pool]);

  return (
    <motion.div
      className="liquidity-calculator"
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.95 }}
    >
      <div className="calculator-header">
        <h3>Liquidity Calculator</h3>
        <button className="close-btn" onClick={onClose}>×</button>
      </div>

      <div className="calculator-body">
        <div className="input-section">
          <div className="calc-input">
            <label>{pool.symbol0} Amount</label>
            <input
              type="number"
              placeholder="0.0"
              value={token0Amount}
              onChange={(e) => setToken0Amount(e.target.value)}
            />
          </div>

          <div className="calc-input">
            <label>{pool.symbol1} Amount</label>
            <input
              type="number"
              placeholder="0.0"
              value={token1Amount}
              onChange={(e) => setToken1Amount(e.target.value)}
            />
          </div>
        </div>

        <div className="results-section">
          <h4>Expected Results</h4>
          <div className="result-row">
            <span>LP Tokens</span>
            <span>{calculatedLp}</span>
          </div>
          <div className="result-row">
            <span>Share of Pool</span>
            <span>{poolShare}%</span>
          </div>
          <div className="result-row">
            <span>Price Ratio</span>
            <span>
              1 {pool.symbol0} = {
                pool.reserve0 && pool.reserve1
                  ? (Number(pool.reserve1) / Number(pool.reserve0)).toFixed(4)
                  : '0'
              } {pool.symbol1}
            </span>
          </div>
        </div>

        <div className="info-section">
          <p>
            💡 Tip: Add liquidity in the same ratio as the pool to minimize slippage.
            Current ratio is {pool.reserve0 && pool.reserve1
              ? `1:${(Number(pool.reserve1) / Number(pool.reserve0)).toFixed(4)}`
              : 'N/A'
            }
          </p>
        </div>
      </div>
    </motion.div>
  );
}

export default LiquidityCalculator;