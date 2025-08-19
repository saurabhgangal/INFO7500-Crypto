// frontend/src/hooks/useAMM.js
import { useContractRead, useContractWrite, useAccount } from 'wagmi';
import { parseEther } from 'viem';
import ammABI from '../abi/SimpleAMM.json';

export function useAMM(poolAddress) {
  const { address } = useAccount();

  // Read functions
  const { data: reserves } = useContractRead({
    address: poolAddress,
    abi: ammABI,
    functionName: 'getReserves',
    watch: true,
  });

  const { data: totalSupply } = useContractRead({
    address: poolAddress,
    abi: ammABI,
    functionName: 'totalSupply',
    watch: true,
  });

  const { data: userBalance } = useContractRead({
    address: poolAddress,
    abi: ammABI,
    functionName: 'balanceOf',
    args: [address],
    enabled: !!address,
    watch: true,
  });

  // Write functions
  const { write: addLiquidity, ...addLiquidityState } = useContractWrite({
    address: poolAddress,
    abi: ammABI,
    functionName: 'addLiquidity',
  });

  const { write: removeLiquidity, ...removeLiquidityState } = useContractWrite({
    address: poolAddress,
    abi: ammABI,
    functionName: 'removeLiquidity',
  });

  const { write: swap, ...swapState } = useContractWrite({
    address: poolAddress,
    abi: ammABI,
    functionName: 'swap',
  });

  return {
    reserves: reserves || [0n, 0n],
    totalSupply: totalSupply || 0n,
    userBalance: userBalance || 0n,
    addLiquidity,
    addLiquidityState,
    removeLiquidity,
    removeLiquidityState,
    swap,
    swapState,
  };
}