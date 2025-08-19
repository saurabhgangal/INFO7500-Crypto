export function useTokens(token0Address, token1Address) {
  return {
    token0: { symbol: 'TKN0', decimals: 18, balance: 0n },
    token1: { symbol: 'TKN1', decimals: 18, balance: 0n },
    isLoading: false,
  };
}
