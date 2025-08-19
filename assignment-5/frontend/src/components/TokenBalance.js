// frontend/src/components/TokenBalance.js
import React from 'react';
import { useAccount, useBalance } from 'wagmi';
import { formatEther } from 'viem';

function TokenBalance({ tokenAddress, symbol }) {
  const { address } = useAccount();
  
  const { data: balance, isLoading } = useBalance({
    address,
    token: tokenAddress,
    watch: true,
  });

  if (!address) return null;

  return (
    <div className="token-balance">
      <span className="balance-label">Balance:</span>
      <span className="balance-value">
        {isLoading ? (
          <span className="loading-dots">...</span>
        ) : (
          <>
            {balance ? formatEther(balance.value) : '0'} {symbol}
          </>
        )}
      </span>
    </div>
  );
}

export default TokenBalance;