async function main() {
  console.log("Starting simple deployment...");
  
  const Factory = await ethers.getContractFactory("AMMFactory");
  console.log("Got factory contract factory");
  
  const factory = await Factory.deploy();
  console.log("Deploy transaction sent");
  
  if (factory.deployed) {
    await factory.deployed();
    console.log("Factory deployed to:", factory.address);
  } else if (factory.waitForDeployment) {
    await factory.waitForDeployment();
    const address = await factory.getAddress();
    console.log("Factory deployed to:", address);
  }
}

main().catch(console.error);
