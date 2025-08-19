async function main() {
  try {
    console.log("Starting deployment...");
    
    const [deployer] = await ethers.getSigners();
    console.log("Deploying contracts with account:", deployer.address);
    console.log("Account balance:", (await deployer.getBalance()).toString());
    
    // Deploy Factory
    console.log("\nDeploying Factory...");
    const Factory = await ethers.getContractFactory("AMMFactory");
    const factory = await Factory.deploy();
    await factory.deployed();
    console.log("Factory deployed to:", factory.address);
    
    console.log("\nDeployment complete!");
  } catch (error) {
    console.error("Error during deployment:");
    console.error(error);
  }
}

main()
  .then(() => {
    console.log("Script finished successfully");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Script failed:");
    console.error(error);
    process.exit(1);
  });
