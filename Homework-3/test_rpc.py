#!/usr/bin/env python3
import requests
import json

def test_rpc():
    print("Testing Bitcoin regtest RPC connection...")
    
    payload = {
        "jsonrpc": "2.0",
        "id": "test",
        "method": "getblockchaininfo",
        "params": []
    }
    
    try:
        response = requests.post(
            "http://127.0.0.1:18443/",  # Correct regtest port
            json=payload,
            auth=("bitcoin", "test123"),
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ RPC Connection successful!")
            print(f"Chain: {result['result']['chain']}")
            print(f"Blocks: {result['result']['blocks']}")
            print(f"Verification progress: {result['result']['verificationprogress']:.2%}")
            return True
        else:
            print(f"❌ RPC Error: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_rpc()
    
    if success:
        print("\n🎉 Ready for sync manager!")
        print("python bitcoin_sync_manager.py \\")
        print("    --rpc-host 127.0.0.1 \\")
        print("    --rpc-port 18443 \\")
        print("    --rpc-user bitcoin \\")
        print("    --rpc-password test123 \\")
        print("    --db-path bitcoin.db")
    else:
        print("\n🔧 Bitcoin Core may not be running. Try:")
        print("./start_bitcoin_regtest.sh")
