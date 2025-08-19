# Assignment 5: Web3 AMM (Automated Market Maker) Application

## 🎯 Project Overview

This is a complete Web3 application that implements an Automated Market Maker (AMM) similar to Uniswap V2. Users can create trading pools, add liquidity, and swap tokens in a decentralized manner.

## 🏗️ Architecture

### Smart Contracts
- **`SimpleAMM.sol`** - Core AMM logic with constant product formula (x*y=k)
- **`AMMFactory.sol`** - Factory contract for deploying and managing AMM pools
- **`AMMRouter.sol`** - Router contract for simplified pool interactions
- **`TestToken.sol`** - ERC20 tokens for testing

### Frontend
- **React.js** with modern UI/UX
- **Wagmi** for Web3 integration
- **Viem** for Ethereum interactions
- **Dark/Light theme** support
- **Mobile responsive** design

## 🚀 Features

✅ **Pool Creation** - Deploy new AMM pools for token pairs  
✅ **Liquidity Management** - Add/remove liquidity from pools  
✅ **Token Swapping** - Swap tokens with automatic price calculation  
✅ **Wallet Integration** - MetaMask and other Web3 wallet support  
✅ **Real-time Data** - Live blockchain data and transaction status  
✅ **Modern UI** - Professional, responsive interface  

## 🛠️ Technology Stack

- **Blockchain**: Ethereum (Hardhat local network)
- **Smart Contracts**: Solidity 0.8.19
- **Frontend**: React.js, Wagmi, Viem
- **Development**: Hardhat, OpenZeppelin Contracts
- **Styling**: Tailwind CSS, React Hot Toast

## 📋 Prerequisites

- Node.js 16+ 
- npm or yarn
- MetaMask browser extension
- Basic understanding of Web3 concepts

## 🚀 Installation & Setup

### 1. Clone the Repository
```bash
git clone https://github.com/saurabhgangal/INFO7500-Crypto.git
cd INFO7500-Crypto/assignment-5
```

### 2. Install Dependencies
```bash
npm install
cd frontend && npm install
cd ..
```

### 3. Start Local Blockchain
```bash
npx hardhat node
```

### 4. Deploy Smart Contracts
```bash
npx hardhat run scripts/deploy.js --network localhost
```

### 5. Start Frontend Application
```bash
cd frontend
npm start
```

## 🎮 Usage Guide

### Connecting Wallet
1. Open the application in your browser
2. Click "Connect Wallet" button
3. Approve MetaMask connection
4. Your wallet address and balance will be displayed

### Creating a Pool
1. Navigate to "Create Pool" tab
2. Enter token addresses (use deployed test tokens)
3. Click "Create Pool"
4. Confirm transaction in MetaMask

### Adding Liquidity
1. Select a pool from "Pool List"
2. Go to "Liquidity" tab
3. Enter amounts for both tokens
4. Click "Add Liquidity"
5. Approve tokens and confirm transaction

### Swapping Tokens
1. Select a pool with liquidity
2. Go to "Swap" tab
3. Choose input token and amount
4. Click "Get Quote" to see expected output
5. Click "Swap" and confirm transaction

## 📁 Project Structure

```
assignment-5/
├── contracts/           # Smart contracts
│   ├── core/           # Core AMM contracts
│   └── test/           # Test tokens
├── frontend/           # React application
│   ├── src/
│   │   ├── components/ # UI components
│   │   ├── abi/        # Contract ABIs
│   │   └── utils/      # Utility functions
│   └── public/         # Static assets
├── scripts/            # Deployment scripts
├── test/              # Smart contract tests
├── hardhat.config.js  # Hardhat configuration
└── README.md          # This file
```

## 🧪 Testing

Run smart contract tests:
```bash
npx hardhat test
```

Run specific test file:
```bash
npx hardhat test test/AMM.test.js
```

## 🔧 Configuration

### Network Configuration
The application is configured for:
- **Localhost**: Hardhat development network
- **Sepolia**: Ethereum testnet (for deployment)
- **Mainnet**: Ethereum mainnet (production)

### Contract Addresses
Contract addresses are automatically managed in `frontend/src/utils/contracts.js` and updated after deployment.

## 🚀 Deployment

### Local Development
```bash
npx hardhat node
npx hardhat run scripts/deploy.js --network localhost
```

### Testnet Deployment
```bash
npx hardhat run scripts/deploy.js --network sepolia
```

## 📱 Demo Features

This application demonstrates:
1. **Web3 Integration** - Full blockchain connectivity
2. **Smart Contract Interaction** - Real-time contract calls
3. **DeFi Functionality** - Pool creation, liquidity, swapping
4. **Modern UI/UX** - Professional, responsive design
5. **Wallet Integration** - MetaMask and Web3 wallet support

## 🎯 Learning Outcomes

- **Smart Contract Development** - Solidity AMM implementation
- **Web3 Frontend** - React + Web3 integration
- **DeFi Concepts** - Automated Market Makers, liquidity pools
- **Blockchain Development** - Hardhat, testing, deployment
- **Modern Web Development** - React, TypeScript, modern tooling

## 🐛 Troubleshooting

### Common Issues
1. **MetaMask not connecting**: Ensure MetaMask is installed and unlocked
2. **Transaction failures**: Check if Hardhat node is running
3. **Contract errors**: Verify contracts are deployed correctly
4. **Frontend build issues**: Clear node_modules and reinstall

### Getting Help
- Check Hardhat console for error messages
- Verify network configuration in MetaMask
- Ensure all dependencies are installed correctly

## 📄 License

This project is for educational purposes as part of INFO7500 Crypto course.

## 👨‍💻 Author

**Saurabh Gangal** - INFO7500 Crypto Course Assignment 5

---

**Note**: This is a demonstration project. Do not use with real funds on mainnet without thorough testing and security audits.
