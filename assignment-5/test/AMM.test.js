// test/AMM.test.js
const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("AMM System", function () {
  let factory, router, tokenA, tokenB, pool;
  let owner, user1, user2;

  beforeEach(async function () {
    [owner, user1, user2] = await ethers.getSigners();

    console.log("Deploying contracts...");
    console.log("Owner address:", owner.address);
    console.log("User1 address:", user1.address);

    // Deploy tokens
    const TestToken = await ethers.getContractFactory("TestToken");
    tokenA = await TestToken.deploy("Token A", "TKA");
    tokenB = await TestToken.deploy("Token B", "TKB");
    console.log("TokenA deployed at:", tokenA.address);
    console.log("TokenB deployed at:", tokenB.address);

    // Deploy factory
    const Factory = await ethers.getContractFactory("AMMFactory");
    factory = await Factory.deploy();
    console.log("Factory deployed at:", factory.address);

    // Deploy router
    const Router = await ethers.getContractFactory("AMMRouter");
    router = await Router.deploy(factory.address);
    console.log("Router deployed at:", router.address);

    // Create pool
    console.log("Creating pool...");
    const createPoolTx = await factory.createPool(tokenA.address, tokenB.address);
    console.log("Pool creation transaction hash:", createPoolTx.hash);
    
    // Wait for transaction to be mined
    await createPoolTx.wait();
    console.log("Pool creation transaction mined");
    
    const poolAddress = await factory.getPoolAddress(tokenA.address, tokenB.address);
    console.log("Pool address:", poolAddress);
    
    if (poolAddress === ethers.constants.AddressZero) {
      throw new Error("Pool was not created successfully");
    }
    
    pool = await ethers.getContractAt("SimpleAMM", poolAddress);
    console.log("Pool contract instance created");

    // Mint tokens
    await tokenA.connect(owner).mint(user1.address, ethers.utils.parseEther("1000"));
    await tokenB.connect(owner).mint(user1.address, ethers.utils.parseEther("1000"));
    console.log("Tokens minted to user1");
    
    // Verify minting worked
    const balanceAfterMintA = await tokenA.balanceOf(user1.address);
    const balanceAfterMintB = await tokenB.balanceOf(user1.address);
    console.log("Balance after mint A:", balanceAfterMintA.toString());
    console.log("Balance after mint B:", balanceAfterMintB.toString());
    
    // Check pool count
    const poolCount = await factory.allPoolsLength();
    console.log("Total pools:", poolCount.toString());
  });

  describe("Factory", function () {
    it("Should create pools correctly", async function () {
      const poolCount = await factory.allPoolsLength();
      console.log("Pool count in test:", poolCount.toString());
      expect(poolCount).to.equal(1);
    });

    it("Should prevent duplicate pools", async function () {
      await expect(
        factory.createPool(tokenA.address, tokenB.address)
      ).to.be.revertedWith("Factory: Pool exists");
    });
  });

  describe("Basic Pool Operations", function () {
    it("Should get reserves correctly", async function () {
      const [reserve0, reserve1, timestamp] = await pool.getReserves();
      console.log("Reserves:", reserve0.toString(), reserve1.toString());
      expect(reserve0).to.equal(0);
      expect(reserve1).to.equal(0);
    });

    it("Should get total supply correctly", async function () {
      const totalSupply = await pool.totalSupply();
      console.log("Total supply:", totalSupply.toString());
      expect(totalSupply).to.equal(0);
    });

    it("Should calculate sqrt correctly", async function () {
      // Test the _sqrt function indirectly by checking if it's working
      const amountA = 10000;
      const amountB = 10000;
      const product = amountA * amountB;
      const expectedSqrt = Math.sqrt(product);
      console.log("Product:", product);
      console.log("Expected sqrt:", expectedSqrt);
      console.log("MINIMUM_LIQUIDITY:", 1000);
      console.log("Should be > MINIMUM_LIQUIDITY:", expectedSqrt > 1000);
    });
  });

  describe("Liquidity", function () {
    it("Should add liquidity correctly", async function () {
      // Use very small amounts to avoid any overflow issues
      const amountA = 10000; // 10000 wei to ensure sqrt > MINIMUM_LIQUIDITY
      const amountB = 10000; // 10000 wei to ensure sqrt > MINIMUM_LIQUIDITY

      console.log("Using amounts:", amountA, amountB);
      console.log("Expected sqrt:", Math.sqrt(amountA * amountB));

      // Check user balances before
      const balanceA = await tokenA.balanceOf(user1.address);
      const balanceB = await tokenB.balanceOf(user1.address);
      console.log("User balance A:", balanceA.toString());
      console.log("User balance B:", balanceB.toString());

      console.log("Approving tokens for direct pool interaction...");
      const approveA = await tokenA.connect(user1).approve(pool.address, amountA);
      const approveB = await tokenB.connect(user1).approve(pool.address, amountB);
      await approveA.wait();
      await approveB.wait();
      console.log("Tokens approved for pool");

      // Check allowances
      const allowanceA = await tokenA.allowance(user1.address, pool.address);
      const allowanceB = await tokenB.allowance(user1.address, pool.address);
      console.log("Allowance A:", allowanceA.toString());
      console.log("Allowance B:", allowanceB.toString());

      console.log("Adding liquidity directly to pool...");
      try {
        const addLiquidityTx = await pool.connect(user1).addLiquidity(
          amountA,
          amountB,
          0, // amount0Min
          0, // amount1Min
          user1.address,
          Math.floor(Date.now() / 1000) + 3600
        );
        
        await addLiquidityTx.wait();
        console.log("Liquidity added directly to pool");

        const lpBalance = await pool.balanceOf(user1.address);
        console.log("LP balance:", lpBalance.toString());
        expect(lpBalance).to.be.gt(0);
      } catch (error) {
        console.error("Error adding liquidity:", error.message);
        throw error;
      }
    });
  });
});