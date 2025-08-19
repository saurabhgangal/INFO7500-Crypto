// scripts/deployTestnet.js
const { ethers } = require("hardhat");
const fs = require("fs");
const path = require("path");

async function main() {
  console.log("🚀 Deploying to Sepolia testnet...\n");
  
  const [deployer] = await ethers.getSigners();
  console.log("Deploying with account:", deployer.address);
  
  const balance = await deployer.getBalance();
  console.log("Account balance:", ethers.utils.formatEther(balance), "ETH");
  
  if (balance.lt(ethers.utils.parseEther("0.1"))) {
    console.error("❌ Insufficient balance. Need at least 0.1 ETH for deployment");
    process.exit(1);
  }

  // Deploy contracts with gas price management
  const gasPrice = await deployer.getGasPrice();
  console.log("Current gas price:", ethers.utils.formatUnits(gasPrice, "gwei"), "gwei");

  const Factory = await ethers.getContractFactory("AMMFactory");
  console.log("\nDeploying Factory...");
  const factory = await Factory.deploy({ gasPrice });
  await factory.deployed();
  console.log("✅ Factory deployed to:", factory.address);

  // Wait for confirmations
  console.log("Waiting for confirmations...");
  await factory.deployTransaction.wait(5);

  // Verify on Etherscan
  console.log("\nVerifying on Etherscan...");
  try {
    await run("verify:verify", {
      address: factory.address,
      constructorArguments: [],
    });
    console.log("✅ Factory verified on Etherscan");
  } catch (error) {
    console.log("❌ Verification failed:", error.message);
  }

  // Continue with router deployment...
  const Router = await ethers.getContractFactory("AMMRouter");
  console.log("\nDeploying Router...");
  const router = await Router.deploy(factory.address, { gasPrice });
  await router.deployed();
  console.log("✅ Router deployed to:", router.address);

  // Save deployment data
  const deploymentData = {
    network: "sepolia",
    chainId: 11155111,
    deployer: deployer.address,
    timestamp: new Date().toISOString(),
    contracts: {
      factory: factory.address,
      router: router.address,
    },
    transactionHashes: {
      factory: factory.deployTransaction.hash,
      router: router.deployTransaction.hash,
    },
  };

  const deploymentsDir = path.join(__dirname, "../deployments");
  if (!fs.existsSync(deploymentsDir)) {
    fs.mkdirSync(deploymentsDir);
  }

  fs.writeFileSync(
    path.join(deploymentsDir, "sepolia.json"),
    JSON.stringify(deploymentData, null, 2)
  );

  console.log("\n✅ Deployment complete!");
  console.log("\nView contracts on Etherscan:");
  console.log(`Factory: https://sepolia.etherscan.io/address/${factory.address}`);
  console.log(`Router: https://sepolia.etherscan.io/address/${router.address}`);
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });