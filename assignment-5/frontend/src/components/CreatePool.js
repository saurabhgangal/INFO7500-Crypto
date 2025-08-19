import React, { useState } from 'react';
import { useContractWrite, useWaitForTransaction, useAccount } from 'wagmi';
import { parseEther } from 'viem';
import toast from 'react-hot-toast';
import factoryABI from '../abi/AMMFactory.json';
import { getContractAddress } from '../utils/contracts';

function CreatePool({ onPoolCreated }) {
  const [token0, setToken0] = useState('');
  const [token1, setToken1] = useState('');
  const { address } = useAccount();

  // Contract addresses - these should come from environment or be configurable
  const FACTORY_ADDRESS = getContractAddress('FACTORY'); // From configuration

  const { write: createPool, data: createPoolData } = useContractWrite({
    address: FACTORY_ADDRESS,
    abi: factoryABI,
    functionName: 'createPool',
  });

  const { isLoading: isCreating, isSuccess: isSuccess } = useWaitForTransaction({
    hash: createPoolData?.hash,
    onSuccess: () => {
      toast.success('Pool created successfully!');
      setToken0('');
      setToken1('');
      onPoolCreated();
    },
    onError: (error) => {
      toast.error(`Failed to create pool: ${error.message}`);
    },
  });

  const handleCreatePool = async () => {
    if (!address) {
      toast.error('Please connect your wallet first');
      return;
    }

    if (!token0 || !token1) {
      toast.error('Please enter both token addresses');
      return;
    }

    if (token0.toLowerCase() === token1.toLowerCase()) {
      toast.error('Token addresses must be different');
      return;
    }

    try {
      createPool({
        args: [token0, token1],
      });
    } catch (error) {
      toast.error(`Error creating pool: ${error.message}`);
    }
  };

  return (
    <div className="create-pool card">
      <h3>Create New Pool</h3>
      <div className="form-group">
        <label>Token 0 Address</label>
        <input
          type="text"
          placeholder="0x..."
          value={token0}
          onChange={(e) => setToken0(e.target.value)}
          disabled={isCreating}
        />
      </div>
      <div className="form-group">
        <label>Token 1 Address</label>
        <input
          type="text"
          placeholder="0x..."
          value={token1}
          onChange={(e) => setToken1(e.target.value)}
          disabled={isCreating}
        />
      </div>
      <button
        onClick={handleCreatePool}
        disabled={isCreating || !token0 || !token1 || !address}
        className="btn-primary"
      >
        {isCreating ? 'Creating...' : 'Create Pool'}
      </button>
      
      {!address && (
        <p className="warning">⚠️ Please connect your wallet to create pools</p>
      )}
    </div>
  );
}

export default CreatePool;
