// frontend/src/components/TransactionHistory.js
import React, { useState, useEffect } from 'react';
import { useAccount } from 'wagmi';
import { motion, AnimatePresence } from 'framer-motion';
import { getTransactions } from '../utils/storage';
import { formatAddress, timeAgo } from '../utils/format';

function TransactionHistory() {
  const { address } = useAccount();
  const [transactions, setTransactions] = useState([]);
  const [filter, setFilter] = useState('all');

  useEffect(() => {
    if (address) {
      const txs = getTransactions(address);
      setTransactions(txs);
    }
  }, [address]);

  const filteredTxs = transactions.filter(tx => {
    if (filter === 'all') return true;
    return tx.type === filter;
  });

  const getTypeIcon = (type) => {
    switch (type) {
      case 'swap': return '💱';
      case 'add': return '➕';
      case 'remove': return '➖';
      default: return '📝';
    }
  };

  const getTypeColor = (type) => {
    switch (type) {
      case 'swap': return '#4CAF50';
      case 'add': return '#2196F3';
      case 'remove': return '#FF9800';
      default: return '#9E9E9E';
    }
  };

  return (
    <div className="transaction-history card">
      <h3>Transaction History</h3>
      
      <div className="tx-filters">
        <button 
          className={filter === 'all' ? 'active' : ''}
          onClick={() => setFilter('all')}
        >
          All
        </button>
        <button 
          className={filter === 'swap' ? 'active' : ''}
          onClick={() => setFilter('swap')}
        >
          Swaps
        </button>
        <button 
          className={filter === 'add' ? 'active' : ''}
          onClick={() => setFilter('add')}
        >
          Add
        </button>
        <button 
          className={filter === 'remove' ? 'active' : ''}
          onClick={() => setFilter('remove')}
        >
          Remove
        </button>
      </div>

      <div className="tx-list">
        <AnimatePresence>
          {filteredTxs.length === 0 ? (
            <div className="no-txs">No transactions yet</div>
          ) : (
            filteredTxs.map((tx, index) => (
              <motion.div
                key={tx.hash}
                className="tx-item"
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: 20 }}
                transition={{ delay: index * 0.05 }}
              >
                <div className="tx-icon" style={{ color: getTypeColor(tx.type) }}>
                  {getTypeIcon(tx.type)}
                </div>
                <div className="tx-details">
                  <div className="tx-main">
                    {tx.type === 'swap' && (
                      <span>
                        {tx.amountIn} {tx.tokenIn} → {tx.amountOut} {tx.tokenOut}
                      </span>
                    )}
                    {tx.type === 'add' && (
                      <span>
                        Added {tx.amount0} {tx.token0} + {tx.amount1} {tx.token1}
                      </span>
                    )}
                    {tx.type === 'remove' && (
                      <span>
                        Removed {tx.lpAmount} LP → {tx.amount0} {tx.token0} + {tx.amount1} {tx.token1}
                      </span>
                    )}
                  </div>
                  <div className="tx-meta">
                    <a 
                      href={`https://etherscan.io/tx/${tx.hash}`}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {formatAddress(tx.hash)}
                    </a>
                    <span className="tx-time">{timeAgo(tx.timestamp)}</span>
                  </div>
                </div>
              </motion.div>
            ))
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}

export default TransactionHistory;