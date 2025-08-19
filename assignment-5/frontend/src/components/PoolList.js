import React, { useState, useEffect } from 'react';
import { useContractReads, useAccount } from 'wagmi';
import factoryABI from '../abi/AMMFactory.json';
import ammABI from '../abi/SimpleAMM.json';
import toast from 'react-hot-toast';
import { getContractAddress } from '../utils/contracts';

function PoolList({ onSelectPool, selectedPool }) {
  const [pools, setPools] = useState([]);
  const [loading, setLoading] = useState(true);
  const { address } = useAccount();

  // Contract addresses - these should come from environment or be configurable
  const FACTORY_ADDRESS = getContractAddress('FACTORY');

  // Read factory data
  const { data: factoryData, isLoading: factoryLoading } = useContractReads({
    contracts: [
      {
        address: FACTORY_ADDRESS,
        abi: factoryABI,
        functionName: 'allPoolsLength',
      },
      {
        address: FACTORY_ADDRESS,
        abi: factoryABI,
        functionName: 'allPools',
        args: [0], // Get first pool
      },
    ],
    watch: true,
  });

  // Fetch pool details
  const { data: poolDetails, isLoading: poolDetailsLoading } = useContractReads({
    contracts: pools.map(pool => ({
      address: pool,
      abi: ammABI,
      functionName: 'getReserves',
    })),
    enabled: pools.length > 0,
    watch: true,
  });

  useEffect(() => {
    if (factoryData && factoryData[0]) {
      const poolCount = Number(factoryData[0]);
      if (poolCount > 0) {
        // Fetch all pool addresses
        const fetchPools = async () => {
          try {
            const poolAddresses = [];
            for (let i = 0; i < poolCount; i++) {
              // This would need to be implemented in the factory contract
              // For now, we'll use the mock data structure
              poolAddresses.push(`0x${i.toString().padStart(40, '0')}`);
            }
            setPools(poolAddresses);
          } catch (error) {
            console.error('Error fetching pools:', error);
            // Fallback to mock data for now
            setMockPools();
          }
        };
        fetchPools();
      } else {
        setMockPools();
      }
    } else if (!factoryLoading) {
      setMockPools();
    }
  }, [factoryData, factoryLoading]);

  const setMockPools = () => {
    const mockPools = [
      {
        id: '1',
        address: '0x1234...5678',
        token0: '0xA0b8...91c2',
        token1: '0xdAC1...7aD3',
        symbol0: 'ETH',
        symbol1: 'USDC',
        tvl: 1500000,
        volume24h: 500000,
        fees24h: 1500,
        reserve0: '500',
        reserve1: '1500000'
      },
      {
        id: '2',
        address: '0x5678...9012',
        token0: '0xB1c9...82d3',
        token1: '0xeAD2...8bE4',
        symbol0: 'WBTC',
        symbol1: 'ETH',
        tvl: 2000000,
        volume24h: 750000,
        fees24h: 2250,
        reserve0: '50',
        reserve1: '800'
      },
      {
        id: '3',
        address: '0x9012...3456',
        token0: '0xC2da...93e4',
        token1: '0xfBE3...9cF5',
        symbol0: 'DAI',
        symbol1: 'USDC',
        tvl: 800000,
        volume24h: 200000,
        fees24h: 600,
        reserve0: '400000',
        reserve1: '400000'
      }
    ];
    setPools(mockPools);
    setLoading(false);
  };

  if (loading || factoryLoading) {
    return (
      <div className="pool-list card">
        <h3>Available Pools</h3>
        <div className="loading">Loading pools...</div>
      </div>
    );
  }

  return (
    <div className="pool-list card">
      <h3>Available Pools</h3>
      {pools.length === 0 ? (
        <p className="no-pools">No pools available. Create one!</p>
      ) : (
        <div className="pools">
          {pools.map((pool) => (
            <div
              key={pool.id || pool}
              className={`pool-item ${selectedPool?.id === pool.id || selectedPool?.address === pool ? 'selected' : ''}`}
              onClick={() => onSelectPool(pool)}
            >
              <div className="pool-name">
                {pool.symbol0 ? `${pool.symbol0}/${pool.symbol1}` : `Pool ${pool.slice(0, 6)}...`}
              </div>
              <div className="pool-address">
                {pool.address || pool}
              </div>
              {pool.tvl && (
                <div className="pool-tvl">
                  TVL: ${(pool.tvl / 1000000).toFixed(2)}M
                </div>
              )}
            </div>
          ))}
        </div>
      )}
      
      {!address && (
        <p className="warning">⚠️ Connect wallet to see real-time pool data</p>
      )}
    </div>
  );
}

export default PoolList;
