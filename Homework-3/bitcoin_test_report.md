# Bitcoin Natural Language to SQL Test Report
============================================================

**Total Tests**: 15
**Passed**: 15
**Failed**: 0
**Success Rate**: 100.0%

## Results by Difficulty:
- Basic: 5/5 passed
- Intermediate: 5/5 passed
- Advanced: 5/5 passed

## Detailed Test Cases:

### Test 1: Basic - ✅ PASS
**Question**: How many blocks are in the database?
**SQL**: `SELECT COUNT(*) FROM blocks;`
**Answer**: COUNT(*): 1
**Description**: Basic count of all blocks

### Test 2: Basic - ✅ PASS
**Question**: How many transactions are there?
**SQL**: `SELECT COUNT(*) FROM transactions;`
**Answer**: COUNT(*): 2
**Description**: Basic count of all transactions

### Test 3: Basic - ✅ PASS
**Question**: What is the highest block height?
**SQL**: `SELECT MAX(height) FROM blocks;`
**Answer**: MAX(height): 800000
**Description**: Simple aggregation function

### Test 4: Basic - ✅ PASS
**Question**: How many transaction outputs are there?
**SQL**: `SELECT COUNT(*) FROM transaction_outputs;`
**Answer**: COUNT(*): 3
**Description**: Count from outputs table

### Test 5: Basic - ✅ PASS
**Question**: What is the total size of all blocks?
**SQL**: `SELECT SUM(size) FROM blocks WHERE size IS NOT NULL;`
**Answer**: SUM(size): 1234567
**Description**: Sum with NULL handling

### Test 6: Intermediate - ✅ PASS
**Question**: What are the latest 5 blocks with their transaction counts?
**SQL**: `SELECT height, hash, nTx, datetime(time, 'unixepoch') as block_time FROM blocks ORDER BY height DESC LIMIT 5;`
**Answer**: height          | hash            | nTx             | block_time     
---------------------------------------------------------------------
800000          | 000000000000... | 2500            | 2023-01-01 0...
**Description**: Ordering with date conversion and limiting

### Test 7: Intermediate - ✅ PASS
**Question**: What is the total amount of transaction fees collected?
**SQL**: `SELECT SUM(fee) FROM transactions WHERE fee IS NOT NULL AND fee > 0;`
**Answer**: SUM(fee): 1e-05
**Description**: Sum with filtering conditions

### Test 8: Intermediate - ✅ PASS
**Question**: How many transactions does each block contain?
**SQL**: `SELECT b.height, b.hash, COUNT(t.txid) as tx_count FROM blocks b LEFT JOIN transactions t ON b.hash = t.block_hash GROUP BY b.hash, b.height ORDER BY b.height DESC LIMIT 10;`
**Answer**: height          | hash            | tx_count       
---------------------------------------------------
800000          | 000000000000... | 2              
**Description**: Join with grouping and counting

### Test 9: Intermediate - ✅ PASS
**Question**: What are the 10 largest transaction outputs by value?
**SQL**: `SELECT txid, value, n as output_index, scriptpubkey_type FROM transaction_outputs WHERE value IS NOT NULL ORDER BY value DESC LIMIT 10;`
**Answer**: txid            | value           | output_index    | scriptpubkey_type
-----------------------------------------------------------------------
1a2b3c4d5e6f... | 6.250000        | 0               | pubkeyhash     
2b3c4d5e6f78... | 0.005000        | 0               | pubkeyhash     
2b3c4d5e6f78... | 0.004500        | 1               | pubkeyhash     
**Description**: Filtering and ordering by value

### Test 10: Intermediate - ✅ PASS
**Question**: Which blocks have more than 1 transaction?
**SQL**: `SELECT height, hash, nTx FROM blocks WHERE nTx > 1 ORDER BY nTx DESC;`
**Answer**: height          | hash            | nTx            
---------------------------------------------------
800000          | 000000000000... | 2500           
**Description**: Filtering with comparison operators

### Test 11: Advanced - ✅ PASS
**Question**: What is the average transaction fee per block?
**SQL**: `SELECT b.height, b.hash, AVG(t.fee) as avg_fee FROM blocks b JOIN transactions t ON b.hash = t.block_hash WHERE t.fee IS NOT NULL AND t.fee > 0 GROUP BY b.hash, b.height ORDER BY b.height DESC LIMIT 10;`
**Answer**: height          | hash            | avg_fee        
---------------------------------------------------
800000          | 000000000000... | 0.000010       
**Description**: Multi-table join with aggregation and grouping

### Test 12: Advanced - ✅ PASS
**Question**: How much Bitcoin value is in each transaction?
**SQL**: `SELECT t.txid, SUM(o.value) as total_output_value FROM transactions t JOIN transaction_outputs o ON t.txid = o.txid WHERE o.value IS NOT NULL GROUP BY t.txid ORDER BY total_output_value DESC LIMIT 10;`
**Answer**: txid            | total_output_value
------------------------------------
1a2b3c4d5e6f... | 6.250000       
2b3c4d5e6f78... | 0.009500       
**Description**: Complex join with aggregation across tables

### Test 13: Advanced - ✅ PASS
**Question**: What is the distribution of transaction types in outputs?
**SQL**: `SELECT scriptpubkey_type, COUNT(*) as count, ROUND(AVG(value), 8) as avg_value FROM transaction_outputs WHERE scriptpubkey_type IS NOT NULL GROUP BY scriptpubkey_type ORDER BY count DESC;`
**Answer**: scriptpubkey_type | count           | avg_value      
-----------------------------------------------------
pubkeyhash      | 3               | 2.086500       
**Description**: Grouping with multiple aggregations

### Test 14: Advanced - ✅ PASS
**Question**: Which addresses have received the most Bitcoin?
**SQL**: `SELECT json_extract(scriptpubkey_addresses, '$[0]') as address, SUM(value) as total_received, COUNT(*) as tx_count FROM transaction_outputs WHERE scriptpubkey_addresses IS NOT NULL AND value IS NOT NULL GROUP BY json_extract(scriptpubkey_addresses, '$[0]') ORDER BY total_received DESC LIMIT 10;`
**Answer**: address         | total_received  | tx_count       
---------------------------------------------------
1BCt3E5q9j2K... | 6.250000        | 1              
1CD4e5f6g7h8... | 0.005000        | 1              
1DE5f6g7h8i9... | 0.004500        | 1              
**Description**: JSON extraction with complex aggregation

### Test 15: Advanced - ✅ PASS
**Question**: What is the relationship between block size and transaction count?
**SQL**: `SELECT b.height, b.size, b.nTx, ROUND(CAST(b.size AS FLOAT) / b.nTx, 2) as avg_tx_size FROM blocks b WHERE b.size IS NOT NULL AND b.nTx > 0 ORDER BY b.height DESC LIMIT 10;`
**Answer**: height          | size            | nTx             | avg_tx_size    
---------------------------------------------------------------------
800000          | 1234567         | 2500            | 493.830000     
**Description**: Mathematical calculations with type casting
