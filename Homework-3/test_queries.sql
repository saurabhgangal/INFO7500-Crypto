.headers on
.mode column
.width 20 10 15 25

SELECT "=== BLOCK INFORMATION ===" as info;
SELECT 
    substr(hash, 1, 20) || '...' as block_hash,
    height,
    nTx as tx_count,
    datetime(time, 'unixepoch') as block_time
FROM blocks;

SELECT "=== TRANSACTIONS ===" as info;
SELECT 
    substr(txid, 1, 20) || '...' as transaction_id,
    fee,
    size as tx_size
FROM transactions;

SELECT "=== TRANSACTION OUTPUTS ===" as info;
SELECT 
    substr(txid, 1, 16) || '...' as tx_id,
    n as output_num,
    value as btc_value,
    scriptpubkey_type as script_type
FROM transaction_outputs
ORDER BY value DESC;

SELECT "=== DATABASE STATS ===" as info;
SELECT 
    'Blocks' as table_name,
    COUNT(*) as record_count
FROM blocks
UNION ALL
SELECT 
    'Transactions',
    COUNT(*)
FROM transactions
UNION ALL
SELECT 
    'Inputs',
    COUNT(*)
FROM transaction_inputs
UNION ALL
SELECT 
    'Outputs',
    COUNT(*)
FROM transaction_outputs;
