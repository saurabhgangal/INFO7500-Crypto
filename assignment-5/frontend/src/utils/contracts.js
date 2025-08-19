// frontend/src/utils/contracts.js
export const CONTRACT_ADDRESSES = {
  // Actual deployed addresses from Hardhat localhost
  FACTORY: '0x9fE46736679d2D9a65F0992F2272dE9f3c7fa6e0',
  ROUTER: '0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9',
  TOKEN_A: '0x5FbDB2315678afecb367f032d93F642f64180aa3',
  TOKEN_B: '0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512',
};

export const NETWORKS = {
  LOCALHOST: {
    chainId: 31337,
    name: 'Hardhat Localhost',
    rpcUrl: 'http://127.0.0.1:8545',
    explorer: null,
  },
  SEPOLIA: {
    chainId: 11155111,
    name: 'Sepolia Testnet',
    rpcUrl: 'https://sepolia.infura.io/v3/YOUR_INFURA_KEY',
    explorer: 'https://sepolia.etherscan.io',
  },
  MAINNET: {
    chainId: 1,
    name: 'Ethereum Mainnet',
    rpcUrl: 'https://mainnet.infura.io/v3/YOUR_INFURA_KEY',
    explorer: 'https://etherscan.io',
  },
};

export const getContractAddress = (contractName) => {
  return CONTRACT_ADDRESSES[contractName] || null;
};

export const getNetworkConfig = (chainId) => {
  return Object.values(NETWORKS).find(network => network.chainId === chainId) || NETWORKS.LOCALHOST;
};
