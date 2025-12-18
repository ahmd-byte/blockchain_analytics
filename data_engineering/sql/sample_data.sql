-- Blockchain Analytics - Sample Data for Development/Testing
-- Use these INSERT statements to populate tables with sample data

-- ===========================================
-- Sample Transactions
-- ===========================================

INSERT INTO `your-project-id.blockchain_analytics.fact_transactions` 
(transaction_hash, block_number, transaction_timestamp, from_address, to_address, value, gas_used, transaction_fee, status)
VALUES
    ('0x001...abc', 1000001, TIMESTAMP('2024-01-01 10:00:00'), '0xwallet001', '0xwallet002', 100.50, 21000, 0.002, 'success'),
    ('0x002...def', 1000002, TIMESTAMP('2024-01-01 11:30:00'), '0xwallet002', '0xwallet003', 250.75, 21000, 0.002, 'success'),
    ('0x003...ghi', 1000003, TIMESTAMP('2024-01-01 14:15:00'), '0xwallet001', '0xwallet004', 500.00, 45000, 0.004, 'success'),
    ('0x004...jkl', 1000004, TIMESTAMP('2024-01-02 09:00:00'), '0xwallet003', '0xwallet001', 75.25, 21000, 0.002, 'success'),
    ('0x005...mno', 1000005, TIMESTAMP('2024-01-02 16:45:00'), '0xwallet005', '0xwallet002', 1000.00, 65000, 0.006, 'success'),
    ('0x006...pqr', 1000006, TIMESTAMP('2024-01-03 08:30:00'), '0xwallet004', '0xwallet005', 300.00, 21000, 0.002, 'success'),
    ('0x007...stu', 1000007, TIMESTAMP('2024-01-03 12:00:00'), '0xwallet002', '0xwallet001', 150.50, 21000, 0.002, 'success'),
    ('0x008...vwx', 1000008, TIMESTAMP('2024-01-04 10:20:00'), '0xwallet006', '0xwallet003', 5000.00, 100000, 0.01, 'success'),
    ('0x009...yza', 1000009, TIMESTAMP('2024-01-04 15:40:00'), '0xwallet001', '0xwallet006', 200.00, 21000, 0.002, 'success'),
    ('0x010...bcd', 1000010, TIMESTAMP('2024-01-05 11:00:00'), '0xwallet007', '0xwallet001', 750.00, 35000, 0.003, 'success');


-- ===========================================
-- Sample Wallets
-- ===========================================

INSERT INTO `your-project-id.blockchain_analytics.dim_wallet`
(wallet_address, first_transaction_date, last_transaction_date, total_transactions, total_volume, unique_counterparties)
VALUES
    ('0xwallet001', DATE('2024-01-01'), DATE('2024-01-05'), 5, 1575.75, 6),
    ('0xwallet002', DATE('2024-01-01'), DATE('2024-01-03'), 4, 1501.75, 4),
    ('0xwallet003', DATE('2024-01-01'), DATE('2024-01-04'), 3, 5326.00, 3),
    ('0xwallet004', DATE('2024-01-01'), DATE('2024-01-03'), 2, 800.00, 2),
    ('0xwallet005', DATE('2024-01-02'), DATE('2024-01-03'), 2, 1300.00, 2),
    ('0xwallet006', DATE('2024-01-04'), DATE('2024-01-04'), 2, 5200.00, 2),
    ('0xwallet007', DATE('2024-01-05'), DATE('2024-01-05'), 1, 750.00, 1);


-- ===========================================
-- Sample Fraud Scores
-- ===========================================

INSERT INTO `your-project-id.blockchain_ml.wallet_fraud_scores`
(wallet_address, fraud_score, is_suspicious, risk_category, tx_count, total_value, last_activity, model_version, flagged_reason)
VALUES
    ('0xwallet001', 0.12, FALSE, 'low', 5, 1575.75, TIMESTAMP('2024-01-05 11:00:00'), 'v1.0.0', NULL),
    ('0xwallet002', 0.25, FALSE, 'low', 4, 1501.75, TIMESTAMP('2024-01-03 12:00:00'), 'v1.0.0', NULL),
    ('0xwallet003', 0.45, FALSE, 'medium', 3, 5326.00, TIMESTAMP('2024-01-04 15:40:00'), 'v1.0.0', NULL),
    ('0xwallet004', 0.18, FALSE, 'low', 2, 800.00, TIMESTAMP('2024-01-03 08:30:00'), 'v1.0.0', NULL),
    ('0xwallet005', 0.55, FALSE, 'medium', 2, 1300.00, TIMESTAMP('2024-01-03 08:30:00'), 'v1.0.0', NULL),
    ('0xwallet006', 0.92, TRUE, 'critical', 2, 5200.00, TIMESTAMP('2024-01-04 15:40:00'), 'v1.0.0', 'High-value rapid transactions detected'),
    ('0xwallet007', 0.75, TRUE, 'high', 1, 750.00, TIMESTAMP('2024-01-05 11:00:00'), 'v1.0.0', 'New wallet with significant transfer');


-- ===========================================
-- Generate More Sample Data (Optional)
-- ===========================================

-- Use this to generate additional sample transactions
-- Adjust the date range and counts as needed

/*
INSERT INTO `your-project-id.blockchain_analytics.fact_transactions`
SELECT
    CONCAT('0x', FORMAT('%03d', n), '...hash') as transaction_hash,
    1000000 + n as block_number,
    TIMESTAMP_ADD(TIMESTAMP('2024-01-01'), INTERVAL n HOUR) as transaction_timestamp,
    CONCAT('0xwallet', FORMAT('%03d', MOD(n, 100))) as from_address,
    CONCAT('0xwallet', FORMAT('%03d', MOD(n + 50, 100))) as to_address,
    ROUND(RAND() * 1000, 2) as value,
    21000 + CAST(RAND() * 50000 AS INT64) as gas_used,
    ROUND(RAND() * 0.01, 4) as transaction_fee,
    'success' as status
FROM UNNEST(GENERATE_ARRAY(1, 1000)) as n;
*/

