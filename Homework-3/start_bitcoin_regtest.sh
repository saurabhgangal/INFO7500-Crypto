#!/bin/bash
echo "Starting Bitcoin Core in regtest mode..."

# Stop any existing Bitcoin processes
pkill bitcoind 2>/dev/null
sleep 2

# Start Bitcoin Core with regtest
bitcoind -regtest \
    -server \
    -rpcuser=bitcoin \
    -rpcpassword=test123 \
    -rpcbind=127.0.0.1:18443 \
    -rpcallowip=127.0.0.1 \
    -fallbackfee=0.0001 \
    -daemon

# Wait for startup
sleep 5

# Check if it's running
if bitcoin-cli -regtest -rpcuser=bitcoin -rpcpassword=test123 getblockchaininfo >/dev/null 2>&1; then
    echo "✅ Bitcoin Core regtest is running"
    block_count=$(bitcoin-cli -regtest -rpcuser=bitcoin -rpcpassword=test123 getblockcount)
    echo "Current block count: $block_count"
    echo "RPC available on: 127.0.0.1:18443"
else
    echo "❌ Failed to start Bitcoin Core"
fi
