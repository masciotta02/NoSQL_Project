
-------------- QUERIES FOR ANALYSIS AND EVALUATION ------------------------------------


1. This query analyzes transactions connected to merchants via the IN_CATEGORY relationship and returns aggregated statistics for each merchant category. 
It calculates the total number of transactions (total) and the number of fraudulent transactions (frauds) per category, 
sorting the results in descending order of fraud occurrences:

    MATCH (t:Transaction)-[:IN_CATEGORY]->(m:Merchant)
    RETURN m.merchant_category, COUNT(t) AS total, 
        SUM(t.fraud_label) AS frauds
    ORDER BY frauds DESC;

OUTPUT:
╒═══════════════════╤═════╤══════╕
│m.merchant_category│total│frauds│
╞═══════════════════╪═════╪══════╡
│"Restaurants"      │9976 │3255  │
├───────────────────┼─────┼──────┤
│"Travel"           │10015│3235  │
├───────────────────┼─────┼──────┤
│"Groceries"        │10019│3217  │
├───────────────────┼─────┼──────┤
│"Clothing"         │10033│3181  │
├───────────────────┼─────┼──────┤
│"Electronics"      │9957 │3179  │
└───────────────────┴─────┴──────┘

STATISTICS: 

- 250117 total DB Hits, where DB Hits means total number of access to databases during the execution of the query

- 106 ms --->  Execution Time


----------------------------------------------------------------------------------------------------------------------------------------------


2. This query analyzes transactions performed by users to identify anomalies based on two criteria:

Unusual Distance : A transaction is flagged if its distance exceeds 1.5 times the user's average transaction distance.
Unusual Device : A transaction is flagged if the device type used is not among the types previously used by the user.

The results include transaction details (ID, user, device type, distance, timestamp) and the anomaly type ("Unusual Distance," "Unusual Device," or "Normal"), 
sorted by timestamp in descending order, limited to the 100 most recent anomalies:

    PROFILE
    MATCH (u:User)-[:PERFORMED]->(t:Transaction)
    WITH u.user_id AS UserID, AVG(t.transaction_distance) AS AvgDistance


    MATCH (u:User {user_id: UserID})-[:PERFORMED]->(t:Transaction)-[:EXECUTED_ON]->(d:Device)
    WITH 
        UserID, 
        COLLECT(DISTINCT d.device_type) AS UsedDevices, 
        AvgDistance

    MATCH (u:User {user_id: UserID})-[:PERFORMED]->(t:Transaction)
    OPTIONAL MATCH (t)-[:EXECUTED_ON]->(d:Device)
    RETURN 
        t.transaction_id AS TransactionID,
        u.user_id AS UserID,
        d.device_type AS DeviceType,
        t.transaction_distance AS Transaction_Distance,
        t.timestamp AS Timestamp,
        CASE
            WHEN t.transaction_distance > (AvgDistance * 1.5) THEN 'Unusual Distance'
            WHEN NOT d.device_type IN UsedDevices THEN 'Unusual Device'
            ELSE 'Normal'
        END AS AnomalyType
    ORDER BY 
        t.timestamp DESC
    LIMIT 100;


Output(Partial for space reasons):

│TransactionID│UserID     │DeviceType│Transaction_Distance│Timestamp            │AnomalyType       
─────────────┼───────────┼──────────┼────────────────────┼─────────────────────┼──────────────────┤
│"TXN_38269"  │"USER_4510"│"Laptop"  │4262.4              │"2023-12-31 23:50:00"│"Unusual Distance"│
├─────────────┼───────────┼──────────┼────────────────────┼─────────────────────┼──────────────────┤
│"TXN_38269"  │"USER_4510"│"Laptop"  │4262.4              │"2023-12-31 23:50:00"│"Unusual Distance"│
├─────────────┼───────────┼──────────┼────────────────────┼─────────────────────┼──────────────────┤
│"TXN_20223"  │"USER_8316"│"Mobile"  │2870.23             │"2023-12-31 23:34:00"│"Normal"          │
├─────────────┼───────────┼──────────┼────────────────────┼─────────────────────┼──────────────────┤
│"TXN_20223"  │"USER_8316"│"Mobile"  │2870.23             │"2023-12-31 23:34:00"│"Normal"          │
├─────────────┼───────────┼──────────┼────────────────────┼─────────────────────┼──────────────────┤
│"TXN_20223"  │"USER_8316"│"Mobile"  │2870.23             │"2023-12-31 23:34:00"│"Normal"          │
├─────────────┼───────────┼──────────┼────────────────────┼─────────────────────┼──────────────────┤

Statistics:

- 2.696.428.477 total DB Hits

- 182.327 ms --->  Execution Time

--------------------------------------------------------------------------------------------------------------------------

3. This query identifies the top combinations of users, locations, device types, and merchant categories involved in high-value fraudulent transactions (amount > 50), 
ranking them within each location based on the total fraud amount. It calculates the number of fraudulent transactions, the total fraud amount, 
and the user's overall fraudulent activity, returning the top 80 results with details such as user ID, location, device type, merchant category, fraud count, total fraud amount,
location-based rank, and the user's total fraud transaction count:

    PROFILE
    MATCH (u:User)-[:PERFORMED]->(t:Transaction)-[:EXECUTED_ON]->(d:Device),
        (t)-[:LOCATED_AT]->(l:Location),
        (t)-[:IN_CATEGORY]->(m:Merchant)
    WHERE t.fraud_label = 1 AND t.amount > 50


    WITH 
        u.user_id AS UserID,
        l.location AS LocationName,
        d.device_type AS DeviceType,
        m.merchant_category AS MerchantCategory,
        COUNT(t) AS FraudulentTxCount,
        SUM(t.amount) AS TotalFraudAmount


    ORDER BY LocationName, TotalFraudAmount DESC

    WITH 
        LocationName,
        COLLECT({
            UserID: UserID,
            DeviceType: DeviceType,
            MerchantCategory: MerchantCategory,
            FraudulentTxCount: FraudulentTxCount,
            TotalFraudAmount: TotalFraudAmount
        }) AS groupedData

    UNWIND RANGE(1, SIZE(groupedData)) AS rank
    WITH 
        groupedData[rank - 1].UserID AS UserID,
        groupedData[rank - 1].DeviceType AS DeviceType,
        groupedData[rank - 1].MerchantCategory AS MerchantCategory,
        groupedData[rank - 1].FraudulentTxCount AS FraudulentTxCount,
        groupedData[rank - 1].TotalFraudAmount AS TotalFraudAmount,
        LocationName,
        rank AS LocationFraudRank


    MATCH (u:User {user_id: UserID})
    WITH 
        UserID, LocationName, DeviceType, MerchantCategory, FraudulentTxCount, TotalFraudAmount, LocationFraudRank,
        SIZE([(u)-[:PERFORMED]->(t2:Transaction) WHERE t2.fraud_label = 1 | t2]) AS TotalUserFraudTx

    WHERE TotalFraudAmount > 55
    RETURN 
        UserID, LocationName, DeviceType, MerchantCategory, FraudulentTxCount, TotalFraudAmount, LocationFraudRank, TotalUserFraudTx
    ORDER BY TotalFraudAmount DESC
    LIMIT 80;

OUTPUT:

UserID     │LocationName│DeviceType│MerchantCategory│FraudulentTxCount│TotalFraudAmount  │LocationFraudRank│TotalUserFraudTx│
╞═══════════╪════════════╪══════════╪════════════════╪═════════════════╪══════════════════╪═════════════════╪════════════════╡
│"USER_2308"│"Tokyo"     │"Tablet"  │"Travel"        │13               │7957.170000000001 │1                │4               │
├───────────┼────────────┼──────────┼────────────────┼─────────────────┼──────────────────┼─────────────────┼────────────────┤
│"USER_2308"│"Tokyo"     │"Tablet"  │"Travel"        │13               │7957.170000000001 │1                │4               │
├───────────┼────────────┼──────────┼────────────────┼─────────────────┼──────────────────┼─────────────────┼────────────────┤
───────────┼────────────┼──────────┼────────────────┼─────────────────┼──────────────────┼─────────────────┼────────────────┤
│"USER_4856"│"Mumbai"    │"Mobile"  │"Electronics"   │10               │7706.0999999999985│1                │6               │
├───────────┼────────────┼──────────┼────────────────┼─────────────────┼──────────────────┼─────────────────┼────────────────┤
├───────────┼────────────┼──────────┼────────────────┼─────────────────┼──────────────────┼─────────────────┼────────────────┤
│"USER_9970"│"New York"  │"Tablet"  │"Electronics"   │8                │6761.28           │1                │3               │
├───────────┼────────────┼──────────┼────────────────┼─────────────────┼──────────────────┼─────────────────┼────────────────┤
│"USER_9970"│"New York"  │"Tablet"  │"Electronics"   │8                │6761.28           │1                │3               │
├───────────┼────────────┼──────────┼────────────────┼─────────────────┼──────────────────┼─────────────────┼────────────────┤

STATISTICS:

- 1778164 total DB Hits

- 406 ms --->  Execution Time


-----------------------------------------------------------------------------------------------------------------------------------------

4. This query identifies pairs of fraudulent transactions performed by the same user, focusing on those that are close in time (within ~500 seconds)
   and have similar amounts (within 10,000).For each pair,it calculates the time difference,counts how many other transactions by the same user have amounts within ±5
   and returns the top matches for further investigation.

   OSSERVATION: We use the APOC library (apoc.date.parse) to convert timestamps (stored as strings in the format "yyyy-MM-dd HH:mm:ss") into milliseconds since epoch. 
                This allows us to calculate the time difference between transactions in milliseconds, which is not natively supported in Cypher for custom date formats. 
                Without APOC, Neo4j cannot directly parse and process custom timestamp formats.

    PROFILE
    MATCH (u:User)-[:PERFORMED]->(t1:Transaction),
        (u)-[:PERFORMED]->(t2:Transaction)
    WHERE t1.fraud_label = 1 AND t2.fraud_label = 1
        AND t1.transaction_id <> t2.transaction_id
        AND abs(apoc.date.parse(t1.timestamp, "ms", "yyyy-MM-dd HH:mm:ss") - 
                apoc.date.parse(t2.timestamp, "ms", "yyyy-MM-dd HH:mm:ss")) <= 500000
        AND abs(t1.amount - t2.amount) <= 10000
    WITH 
        t1, t2, u.user_id AS UserID,
        t1.transaction_id AS Tx1_ID, t1.amount AS Tx1_Amount, t1.timestamp AS Tx1_Time,
        t2.transaction_id AS Tx2_ID, t2.amount AS Tx2_Amount, t2.timestamp AS Tx2_Time,
        abs(apoc.date.parse(t1.timestamp, "ms", "yyyy-MM-dd HH:mm:ss") - 
            apoc.date.parse(t2.timestamp, "ms", "yyyy-MM-dd HH:mm:ss")) AS TimeDiff_Seconds
    OPTIONAL MATCH (u)-[:PERFORMED]->(t3:Transaction)
    WHERE t3.amount >= t1.amount - 5 
        AND t3.amount <= t1.amount + 5
    WITH 
        Tx1_ID, UserID, Tx1_Amount, Tx1_Time, Tx2_ID, Tx2_Amount, Tx2_Time, TimeDiff_Seconds,
        COUNT(DISTINCT t3) AS SimilarAmountTxCount
    ORDER BY SimilarAmountTxCount DESC, TimeDiff_Seconds ASC
    LIMIT 1000
    RETURN 
        Tx1_ID, UserID, Tx1_Amount, Tx1_Time, Tx2_ID, Tx2_Amount, Tx2_Time, TimeDiff_Seconds, SimilarAmountTxCount;


OUTPUT:

╒═══════════╤═══════════╤══════════╤═════════════════════╤═══════════╤══════════╤═════════════════════╤════════════════╤════════════════════╕
│Tx1_ID     │UserID     │Tx1_Amount│Tx1_Time             │Tx2_ID     │Tx2_Amount│Tx2_Time             │TimeDiff_Seconds│SimilarAmountTxCount│
╞═══════════╪═══════════╪══════════╪═════════════════════╪═══════════╪══════════╪═════════════════════╪════════════════╪════════════════════╡
│"TXN_18555"│"USER_4953"│5.6       │"2023-06-16 04:46:00"│"TXN_43217"│176.1     │"2023-06-16 04:42:00"│240000          │4731                │
├───────────┼───────────┼──────────┼─────────────────────┼───────────┼──────────┼─────────────────────┼────────────────┼────────────────────┤
│"TXN_43217"│"USER_4953"│176.1     │"2023-06-16 04:42:00"│"TXN_18555"│5.6       │"2023-06-16 04:46:00"│240000          │864                 │
└───────────┴───────────┴──────────┴─────────────────────┴───────────┴──────────┴─────────────────────┴────────────────┴────────────────────┘

STATISTICS:

- 3.682.838 total DB Hits

- 2.257 ms --->  Execution Time


---------------------------------------------------------------------------------------------------------------------------------------------------------------


5. This query retrieves up to 20,000 random combinations of users (User), transactions (Transaction), devices (Device), and merchants (Merchant) from the database. 
   It returns the user_id of the user, transaction_id of the transaction, device_type of the device, and merchant_category of the merchant. 
   It was designed to compare the performance of a graph database (Neo4j) with a relational database, 
   which is typically more efficient at handling queries involving large-scale, unfiltered cross-joins of this nature:

    PROFILE
    MATCH (u:User), (t:Transaction), (d:Device), (m:Merchant)
    RETURN u.user_id, t.transaction_id, d.device_type, m.merchant_category
    LIMIT 20000;

OUTPUT:

╒═══════════╤════════════════╤═════════════╤═══════════════════╕
│u.user_id  │t.transaction_id│d.device_type│m.merchant_category│
╞═══════════╪════════════════╪═════════════╪═══════════════════╡
│"USER_5829"│"TXN_33553"     │"Laptop"     │"Travel"           │
├───────────┼────────────────┼─────────────┼───────────────────┤
│"USER_5829"│"TXN_9427"      │"Laptop"     │"Travel"           │
├───────────┼────────────────┼─────────────┼───────────────────┤
│"USER_5829"│"TXN_199"       │"Laptop"     │"Travel"           │
├───────────┼────────────────┼─────────────┼───────────────────┤


STATISTICS:

- 6174 total DB Hits

- 6284 ms --->  Execution Time

------------------------------------------------------------------------------------------------------------------------------


6. This query categorizes transactions into three groups based on card_age: 

   - 'New' (1–24 months), 
   - 'Medium' (25–48 months), 
   - 'Old' (older than 48 months). 
   
   For each category, it calculates:

   The total number of transactions (TotalTransactions).
   The number of fraudulent transactions (FraudulentTransactions).
   The fraud ratio as a percentage (FraudRatio), which is the proportion of fraudulent transactions relative to the total.
   The results are grouped by Card_Category, ordered by FraudRatio in descending order, highlighting categories with the highest fraud risk:


   PROFILE
   MATCH (t:Transaction)
   WITH 
        CASE
            WHEN t.card_age >= 1 AND t.card_age <= 24 THEN 'New'       // Cards aged 1-24 months
            WHEN t.card_age >= 25 AND t.card_age <= 48 THEN 'Medium'   // Cards aged 25-48 months (2-4 years)
            ELSE 'Old'                                                 // Cards older than 48 months (4+ years)
        END AS Card_Category,
        COUNT(t) AS TotalTransactions,
        SUM(CASE WHEN t.fraud_label = 1 THEN 1 ELSE 0 END) AS FraudulentTransactions
    WITH 
        Card_Category, 
        TotalTransactions, 
        FraudulentTransactions,
        toFloat(FraudulentTransactions) / TotalTransactions * 100 AS FraudRatio
    RETURN 
        Card_Category, 
        TotalTransactions, 
        FraudulentTransactions, 
        FraudRatio
    ORDER BY 
        FraudRatio DESC;

OUTPUT:

═════════════╤═════════════════╤══════════════════════╤═════════════════╕
│Card_Category│TotalTransactions│FraudulentTransactions│FraudRatio       │
╞═════════════╪═════════════════╪══════════════════════╪═════════════════╡
│"Medium"     │5045             │1648                  │32.66600594648166│
├─────────────┼─────────────────┼──────────────────────┼─────────────────┤
│"New"        │4984             │1601                  │32.12279293739968│
├─────────────┼─────────────────┼──────────────────────┼─────────────────┤
│"Old"        │39971            │12818                 │32.06824948087363│
└─────────────┴─────────────────┴──────────────────────┴─────────────────┘

STATISTICS:

- 250001 total  DB Hits

- 88 ms --->  Execution Time

------------------------------------------------------------------------------------------------------------------------------


7. This query analyzes user transactions, grouping them by UserID. For each user, it calculates the total number of transactions and the count of fraudulent transactions. 
   It then filters for users with more than 5 fraudulent transactions, returning the top 10 users sorted by fraud count (descending) and total transactions (ascending):

   PROFILE
   MATCH (u:User)-[:PERFORMED]->(t:Transaction)
   WITH 
        u.user_id AS UserID,
        COUNT(t) AS total_transactions,
        SUM(t.fraud_label) AS fraud_count
   RETURN 
        UserID, 
        total_transactions, 
        fraud_count
   ORDER BY 
        fraud_count DESC, 
        total_transactions ASC
   LIMIT 10;

OUTPUT:

╒═══════════╤══════════════════╤═══════════╕
│UserID     │total_transactions│fraud_count│
╞═══════════╪══════════════════╪═══════════╡
│"USER_5780"│196               │98         │
├───────────┼──────────────────┼───────────┤
│"USER_7704"│169               │91         │
├───────────┼──────────────────┼───────────┤
│"USER_3800"│169               │91         │
├───────────┼──────────────────┼───────────┤
│"USER_7026"│121               │88         │
├───────────┼──────────────────┼───────────┤
│"USER_2403"│144               │84         │
├───────────┼──────────────────┼───────────┤
│"USER_7671"│144               │84         │
├───────────┼──────────────────┼───────────┤
│"USER_7473"│144               │84         │
├───────────┼──────────────────┼───────────┤
│"USER_2287"│144               │84         │
├───────────┼──────────────────┼───────────┤
│"USER_4083"│169               │78         │
├───────────┼──────────────────┼───────────┤
│"USER_3934"│169               │78         │
└───────────┴──────────────────┴───────────┘

STATISTICS:

- 1838813 total DB Hits

- 255 ms --->  Execution Time


----------------------------------------------------------------------------------------------------------------------------------------

8. This query identifies the top 10 users with the highest number of transactions. It groups transactions by UserID, counts the total transactions for each user 
   and returns the results sorted in descending order of transaction count:

    PROFILE
    MATCH (u:User)-[:PERFORMED]->(t:Transaction)
    WITH u.user_id AS UserID, COUNT(t) AS TotalTransactions
    RETURN UserID, TotalTransactions
    ORDER BY TotalTransactions DESC
    LIMIT 10;

OUTPUT:

╒═══════════╤═════════════════╕
│UserID     │TotalTransactions│
╞═══════════╪═════════════════╡
│"USER_6599"│256              │
├───────────┼─────────────────┤
│"USER_3925"│256              │
├───────────┼─────────────────┤
│"USER_9998"│256              │
├───────────┼─────────────────┤
│"USER_3415"│225              │
├───────────┼─────────────────┤
│"USER_1027"│225              │
├───────────┼─────────────────┤
│"USER_5014"│225              │
├───────────┼─────────────────┤
│"USER_6237"│196              │
├───────────┼─────────────────┤
│"USER_2620"│196              │
├───────────┼─────────────────┤
│"USER_6243"│196              │
├───────────┼─────────────────┤
│"USER_5780"│196              │
└───────────┴─────────────────┘


STATISTICS:

- 1738281 total DB Hits

- 485 ms --->  Execution Time

