--Cons for Relation DBMS (mange multi-join and self-join)

--1 This query is designed to detect users with suspicious behavior by analyzing their transaction patterns 
SELECT 
    UserID, 
    COUNT(*) AS total_transactions,
    SUM(Fraud_Label) AS fraud_count
FROM 
    transactions_N
GROUP BY 
    UserID
HAVING 
    fraud_count > 5
ORDER BY fraud_count DESC, total_transactions
LIMIT 10;

--0.13
--OUTPUT: 
+-----------+--------------------+-------------+
| UserID    | total_transactions | fraud_count |
+-----------+--------------------+-------------+
| USER_7026 |                 11 |           8 |
| USER_1676 |                  8 |           7 |
| USER_3933 |                 10 |           7 |
| USER_6976 |                 10 |           7 |
| USER_6179 |                 10 |           7 |
| USER_1689 |                 10 |           7 |
| USER_1560 |                 10 |           7 |
| USER_3670 |                 10 |           7 |
| USER_7115 |                 10 |           7 |
| USER_9983 |                 10 |           7 |
+-----------+--------------------+-------------+

-- 2 This query is designed to identify which merchant categories are most prone to fraud by analyzing the number 
--of fraudulent transactions (Fraud_Label = 1) for each category. 
SELECT 
    MerchantCategory, 
    COUNT(*) AS total, 
    SUM(Fraud_Label) AS frauds
FROM 
    transactions_N t, Merchants m
WHERE t.MerchantID = m.MerchantID
GROUP BY 
    MerchantCategory
ORDER BY 
    frauds DESC;

--0.11
--OUTPUT
+------------------+-------+--------+
| MerchantCategory | total | frauds |
+------------------+-------+--------+
| Restaurants      |  9976 |   3255 |
| Travel           | 10015 |   3235 |
| Groceries        | 10019 |   3217 |
| Clothing         | 10033 |   3181 |
| Electronics      |  9957 |   3179 |
+------------------+-------+--------+


--3 This query identifies transactions that are flagged as either:
--"Unusual Distance" : A transaction where the distance exceeds 1.5 times the user's average transaction distance.
WITH UserStats AS (
    -- Calculate average transaction distance for each user
    SELECT 
        UserID,
        AVG(Transaction_Distance) AS AvgDistance
    FROM 
        transactions_N
    GROUP BY 
        UserID
)
SELECT 
    t.TransactionID,
    t.UserID,
    d.DeviceType,
    t.Transaction_Distance,
    t.Timestamp,
    CASE 
        WHEN t.Transaction_Distance > (us.AvgDistance * 1.5) THEN 'Unusual Distance'
        WHEN t.DeviceID NOT IN (
            SELECT DISTINCT t2.DeviceID
            FROM transactions_N t2
            WHERE t2.UserID = t.UserID
        ) THEN 'Unusual Device'
        ELSE 'Normal'
    END AS AnomalyType
FROM 
    transactions_N t
LEFT JOIN 
    Devices d ON t.DeviceID = d.DeviceID -- Use LEFT JOIN to avoid missing data
LEFT JOIN 
    UserStats us ON t.UserID = us.UserID -- Use LEFT JOIN to avoid missing stats
WHERE 
    t.Transaction_Distance > (us.AvgDistance * 1.5) -- Unusual Distance
    OR t.DeviceID NOT IN (
        SELECT DISTINCT t2.DeviceID
        FROM transactions_N t2
        WHERE t2.UserID = t.UserID
    ) -- Unusual Device
ORDER BY 
    t.Timestamp DESC
LIMIT 100; -- Limit to 100 most recent anomalies

--4.15/3.97
--OUTPUT
+---------------+-----------+------------+----------------------+---------------------+------------------+
| TransactionID | UserID    | DeviceType | Transaction_Distance | Timestamp           | AnomalyType      |
+---------------+-----------+------------+----------------------+---------------------+------------------+
| TXN_38269     | USER_4510 | Laptop     |              4262.40 | 2023-12-31 23:50:00 | Unusual Distance |
| TXN_25394     | USER_8356 | Laptop     |              4779.84 | 2023-12-31 21:52:00 | Unusual Distance |

--4  Identifies whether newer or older cards are more prone to fraud.
SELECT 
    CASE 
        WHEN Card_Age BETWEEN 1 AND 24 THEN 'New'       -- Cards aged 1-24 months
        WHEN Card_Age BETWEEN 25 AND 48 THEN 'Medium'   -- Cards aged 25-48 months (2-4 years)
        ELSE 'Old'                                      -- Cards older than 48 months (4+ years)
    END AS Card_Category,
    COUNT(*) AS TotalTransactions,
    SUM(CASE WHEN Fraud_Label = 1 THEN 1 ELSE 0 END) AS FraudulentTransactions,
    (SUM(CASE WHEN Fraud_Label = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS FraudRatio -- Fraud Ratio in %
FROM 
    transactions_N
GROUP BY 
    Card_Category
ORDER BY 
    FraudRatio DESC; -- Order by Fraud Ratio in descending order

--0.13
--OUTPUT 
+---------------+-------------------+------------------------+------------+
| Card_Category | TotalTransactions | FraudulentTransactions | FraudRatio |
+---------------+-------------------+------------------------+------------+
| Medium        |              5045 |                   1648 |    32.6660 |
| New           |              4984 |                   1601 |    32.1228 |
| Old           |             39971 |                  12818 |    32.0682 |
+---------------+-------------------+------------------------+------------+

--5 The top 10 users based on # of transactions
SELECT 
    UserID, 
    COUNT(TransactionID) AS TotalTransactions
FROM 
    transactions_N
GROUP BY 
    UserID
ORDER BY TotalTransactions
LIMIT 10;

--0.11
--OUTPUT
+-----------+-------------------+
| UserID    | TotalTransactions |
+-----------+-------------------+
| USER_9998 |                16 |
| USER_3925 |                16 |
| USER_6599 |                16 |
| USER_3415 |                15 |
| USER_5014 |                15 |
| USER_1027 |                15 |
| USER_6229 |                14 |
| USER_2620 |                14 |
| USER_6700 |                14 |
| USER_4343 |                14 |
+-----------+-------------------+

-- Access different tables (better in MySQL than Neo4J)
SELECT 
    u.UserID,
    t.TransactionID,
    d.DeviceID,
    m.MerchantID
FROM 
    transactions_N t
JOIN 
    Users u ON t.UserID = u.UserID
JOIN 
    Devices d ON t.DeviceID = d.DeviceID
JOIN 
    Merchants m ON t.MerchantID = m.MerchantID
LIMIT 20000;

--0.14/0.12
+-----------+---------------+----------+------------+
| UserID    | TransactionID | DeviceID | MerchantID |
+-----------+---------------+----------+------------+
| USER_8270 | TXN_0         |        1 |          1 |
| USER_1860 | TXN_1         |        2 |          2 |
...

-- SELFJOIN (high time complexity in MySQL)
SELECT 
    t1.TransactionID AS Tx1_ID, 
    t1.UserID, 
    t1.Transaction_Amount AS Tx1_Amount, 
    t1.Timestamp AS Tx1_Time, 
    t2.TransactionID AS Tx2_ID, 
    t2.Transaction_Amount AS Tx2_Amount, 
    t2.Timestamp AS Tx2_Time, 
    TIMESTAMPDIFF(SECOND, t1.Timestamp, t2.Timestamp) AS TimeDiff_Seconds,
    COUNT(*) OVER (PARTITION BY t1.UserID) AS TxPerUser,
    (
        SELECT COUNT(*)
        FROM transactions_N t3
        WHERE t3.UserID = t1.UserID 
          AND t3.Transaction_Amount BETWEEN t1.Transaction_Amount - 5 AND t1.Transaction_Amount + 5
    ) AS SimilarAmountTxCount
FROM 
    transactions_N t1
JOIN 
    transactions_N t2 
    ON t1.UserID = t2.UserID 
    AND t1.TransactionID <> t2.TransactionID 
    AND ABS(TIMESTAMPDIFF(SECOND, t1.Timestamp, t2.Timestamp)) <= 500000 
    AND ABS(t1.Transaction_Amount - t2.Transaction_Amount) <= 10000
WHERE 
    t1.Fraud_Label = 1 
    AND t2.Fraud_Label = 1
ORDER BY 
    SimilarAmountTxCount DESC, 
    TimeDiff_Seconds ASC
LIMIT 1000;

--28.46
--OUTPUT
+-----------+-----------+------------+---------------------+-----------+------------+---------------------+------------------+-----------+----------------------+
| Tx1_ID    | UserID    | Tx1_Amount | Tx1_Time            | Tx2_ID    | Tx2_Amount | Tx2_Time            | TimeDiff_Seconds | TxPerUser | SimilarAmountTxCount |
+-----------+-----------+------------+---------------------+-----------+------------+---------------------+------------------+-----------+----------------------+
| TXN_20267 | USER_2308 |      40.02 | 2023-12-01 10:51:00 | TXN_1037  |     612.09 | 2023-12-02 22:43:00 |           129120 |         2 |                    6 |
| TXN_28223 | USER_6378 |      13.41 | 2023-03-11 08:54:00 | TXN_11541 |     102.24 | 2023-03-12 00:49:00 |            57300 |         2 |                    5 |
...

--MULTIJOIN (problem in MySQL)
SELECT 
    u.UserID, 
    l.LocationName, 
    d.DeviceType, 
    m.MerchantCategory, 
    COUNT(t.TransactionID) AS FraudulentTxCount, 
    SUM(t.Transaction_Amount) AS TotalFraudAmount,
    RANK() OVER (PARTITION BY l.LocationName ORDER BY SUM(t.Transaction_Amount) DESC) AS LocationFraudRank,
    (
        SELECT COUNT(*)
        FROM transactions_N t2
        WHERE t2.UserID = u.UserID 
          AND t2.Fraud_Label = 1
    ) AS TotalUserFraudTx
FROM 
    Users u
JOIN 
    transactions_N t ON u.UserID = t.UserID
JOIN 
    Locations l ON t.LocationID = l.LocationID
JOIN 
    Devices d ON t.DeviceID = d.DeviceID
JOIN 
    Merchants m ON t.MerchantID = m.MerchantID
WHERE 
    t.Fraud_Label = 1 
    AND t.Transaction_Amount > 50
GROUP BY 
    u.UserID, 
    l.LocationName, 
    d.DeviceType, 
    m.MerchantCategory
HAVING 
    SUM(t.Transaction_Amount) > 55
ORDER BY 
    TotalFraudAmount DESC
LIMIT 500;

--4min 37.7 sec
--OUTPUT
+-----------+--------------+------------+------------------+-------------------+------------------+-------------------+------------------+
| UserID    | LocationName | DeviceType | MerchantCategory | FraudulentTxCount | TotalFraudAmount | LocationFraudRank | TotalUserFraudTx |
+-----------+--------------+------------+------------------+-------------------+------------------+-------------------+------------------+
| USER_1988 | New York     | Tablet     | Electronics      |                 1 |          1005.32 |                 1 |                2 |
| USER_1302 | London       | Tablet     | Electronics      |                 1 |           971.61 |                 1 |                2 |
