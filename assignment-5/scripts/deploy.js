const { ethers } = require("hardhat");

async function main() {
  console.log("🚀 Starting deployment...");

  // Get deployer account
  const [deployer] = await ethers.getSigners();
  console.log("Deploying with account:", deployer.address);

  // Check balance
  const balance = await deployer.getBalance();
  console.log("Account balance:", ethers.utils.formatEther(balance), "ETH");

  try {
    // Deploy TestToken A
    console.log("\n📦 Deploying TokenA...");
    const TokenA = await ethers.getContractFactory("TestToken");
    const tokenA = await TokenA.deploy("Test Token A", "TKNA");
    await tokenA.deployed();
    console.log("TokenA deployed to:", tokenA.address);

    // Deploy TestToken B
    console.log("\n📦 Deploying TokenB...");
    const TokenB = await ethers.getContractFactory("TestToken");
    const tokenB = await TokenB.deploy("Test Token B", "TKNB");
    await tokenB.deployed();
    console.log("TokenB deployed to:", tokenB.address);

    // Deploy AMMFactory
    console.log("\n📦 Deploying AMMFactory...");
    const AMMFactory = await ethers.getContractFactory("AMMFactory");
    const factory = await AMMFactory.deploy();
    await factory.deployed();
    console.log("AMMFactory deployed to:", factory.address);

    // Deploy AMMRouter
    console.log("\n📦 Deploying AMMRouter...");
    const AMMRouter = await ethers.getContractFactory("AMMRouter");
    const router = await AMMRouter.deploy(factory.address);
    await router.deployed();
    console.log("AMMRouter deployed to:", router.address);

    // Create a pool
    console.log("\n🏊 Creating pool...");
    const createPoolTx = await factory.createPool(tokenA.address, tokenB.address);
    const receipt = await createPoolTx.wait();
    console.log("Pool creation transaction:", receipt.transactionHash);

    console.log("\n📋 Deployment Summary:");
    console.log("====================");
    console.log("TokenA:", tokenA.address);
    console.log("TokenB:", tokenB.address);
    console.log("Factory:", factory.address);
    console.log("Router:", router.address);
    console.log("Deployer:", deployer.address);

    // Save addresses to file for frontend
    const fs = require('fs');
    const addresses = {
      tokenA: tokenA.address,
      tokenB: tokenB.address,
      factory: factory.address,
      router: router.address,
      deployer: deployer.address
    };
    
    fs.writeFileSync('deployed-addresses.json', JSON.stringify(addresses, null, 2));
    console.log("\n💾 Addresses saved to deployed-addresses.json");

  } catch (error) {
    console.error("❌ Deployment failed:", error);
    process.exit(1);
  }
}

main()
  .then(() => {
    console.log("\n✅ Deployment completed successfully!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("❌ Deployment script failed:", error);
    process.exit(1);
  });
