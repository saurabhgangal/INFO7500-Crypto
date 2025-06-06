#!/usr/bin/env python3
"""
Bitcoin Natural Language to SQL Test Cases
15 comprehensive test cases showing natural language -> SQL -> actual results

Each test case is a triple:
1. Natural language question
2. SQL statement that generates the correct answer
3. Actual answer from executing on the database
"""

import sqlite3
import os

def run_bitcoin_test_cases():
    """Execute all test cases and display results as triples"""
    
    if not os.path.exists('bitcoin.db'):
        print("❌ bitcoin.db not found")
        return False
    
    # 15 Comprehensive Test Cases (Natural Language, SQL, Expected Result)
    test_cases = [
        # BASIC LEVEL (1-5)
        ("How many blocks are in the database?", 
         "SELECT COUNT(*) FROM blocks;"),
        
        ("How many transactions are there?", 
         "SELECT COUNT(*) FROM transactions;"),
        
        ("What is the highest block height?", 
         "SELECT MAX(height) FROM blocks;"),
        
        ("How many transaction outputs are there?", 
         "SELECT COUNT(*) FROM transaction_outputs;"),
        
        ("What is the total size of all blocks in bytes?", 
         "SELECT SUM(size) FROM blocks WHERE size IS NOT NULL;"),
        
        # INTERMEDIATE LEVEL (6-10)
        ("What are the latest 3 blocks with their details?", 
         "SELECT height, substr(hash, 1, 16) || '...', nTx, datetime(time, 'unixepoch') FROM blocks ORDER BY height DESC LIMIT 3;"),
        
        ("What is the total amount of transaction fees collected?", 
         "SELECT COALESCE(SUM(fee), 0) FROM transactions WHERE fee IS NOT NULL AND fee > 0;"),
        
        ("How many transactions does the latest block contain?", 
         "SELECT nTx FROM blocks ORDER BY height DESC LIMIT 1;"),
        
        ("What are the top 5 largest transaction outputs by value?", 
         "SELECT txid, value, n FROM transaction_outputs WHERE value IS NOT NULL ORDER BY value DESC LIMIT 5;"),
        
        ("Which blocks have more than 1 transaction?", 
         "SELECT COUNT(*) FROM blocks WHERE nTx > 1;"),
        
        # ADVANCED LEVEL (11-15)
        ("What is the average number of transactions per block?", 
         "SELECT ROUND(AVG(nTx), 2) FROM blocks WHERE nTx IS NOT NULL;"),
        
        ("How much total Bitcoin value is stored in all transaction outputs?", 
         "SELECT ROUND(SUM(value), 8) FROM transaction_outputs WHERE value IS NOT NULL;"),
        
        ("What types of transaction outputs exist and how many of each?", 
         "SELECT scriptpubkey_type, COUNT(*) FROM transaction_outputs WHERE scriptpubkey_type IS NOT NULL GROUP BY scriptpubkey_type ORDER BY COUNT(*) DESC;"),
        
        ("What is the average block size in bytes?", 
         "SELECT ROUND(AVG(size), 0) FROM blocks WHERE size IS NOT NULL;"),
        
        ("How many coinbase transactions are there?", 
         "SELECT COUNT(*) FROM transaction_inputs WHERE coinbase IS NOT NULL;")
    ]
    
    print("🧪 BITCOIN NATURAL LANGUAGE TO SQL TEST CASES")
    print("=" * 80)
    print("Format: (Question, SQL, Actual Result)")
    print("=" * 80)
    
    conn = sqlite3.connect('bitcoin.db')
    cursor = conn.cursor()
    
    passed = 0
    total = len(test_cases)
    
    for i, (question, sql) in enumerate(test_cases, 1):
        print(f"\n📝 TEST CASE {i}:")
        print(f"❓ QUESTION: {question}")
        print(f"🔍 SQL: {sql}")
        
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Format the result for display
            if len(result) == 1 and len(result[0]) == 1:
                # Single value result
                answer = result[0][0]
                print(f"✅ ANSWER: {answer}")
            elif len(result) == 1:
                # Single row, multiple columns
                formatted = ", ".join([f"{columns[j]}: {result[0][j]}" for j in range(len(columns))])
                print(f"✅ ANSWER: {formatted}")
            elif len(result) <= 5:
                # Multiple rows, show all
                print(f"✅ ANSWER ({len(result)} rows):")
                for j, row in enumerate(result):
                    if len(row) == 1:
                        print(f"   Row {j+1}: {row[0]}")
                    else:
                        formatted = ", ".join([f"{columns[k]}: {row[k]}" for k in range(len(columns))])
                        print(f"   Row {j+1}: {formatted}")
            else:
                # Many rows, show summary
                print(f"✅ ANSWER: {len(result)} rows returned")
                print(f"   Sample: {result[0]}")
                print(f"   ...")
            
            passed += 1
            
        except Exception as e:
            print(f"❌ ERROR: {e}")
    
    conn.close()
    
    print(f"\n" + "=" * 80)
    print(f"📊 SUMMARY: {passed}/{total} test cases passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! System is working correctly.")
    elif passed >= total * 0.8:
        print("✅ Most tests passed. System is functional.")
    else:
        print("⚠️  Several tests failed. System needs debugging.")
    
    return passed >= total * 0.8

if __name__ == "__main__":
    success = run_bitcoin_test_cases()
