require("@nomicfoundation/hardhat-toolbox");

// Try to load solidity-coverage, but don't fail if it's not available
try {
  require("solidity-coverage");
} catch (e) {
  console.log("solidity-coverage not found. Install it with: npm install --save-dev solidity-coverage");
}

/** @type import('hardhat/config').HardhatUserConfig */
module.exports = {
  solidity: {
    version: "0.8.20",
    settings: {
      optimizer: {
        enabled: true,
        runs: 200
      }
    }
  },
  networks: {
    hardhat: {
      allowUnlimitedContractSize: true
    }
  },
  gasReporter: {
    enabled: process.env.REPORT_GAS !== undefined,
    currency: "USD"
  },
  paths: {
    sources: "./contracts",
    tests: "./test",
    cache: "./cache",
    artifacts: "./artifacts"
  }
};