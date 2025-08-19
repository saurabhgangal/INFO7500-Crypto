// scripts/verify.js
const { run } = require("hardhat");
const deployments = require("../deployments/sepolia.json");

async function main() {
  console.log("Verifying contracts on Etherscan...\n");

  // Verify Factory
  try {
    await run("verify:verify", {
      address: deployments.contracts.factory,
      constructorArguments: [],
    });
    console.log("✅ Factory verified");
  } catch (error) {
    console.log("Factory verification error:", error.message);
  }

  // Verify Router
  try {
    await run("verify:verify", {
      address: deployments.contracts.router,
      constructorArguments: [deployments.contracts.factory],
    });
    console.log("✅ Router verified");
  } catch (error) {
    console.log("Router verification error:", error.message);
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });