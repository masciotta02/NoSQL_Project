// LOADING OF CSV FILE
LOAD CSV WITH HEADERS FROM 'file:///fraud_dataset.csv' AS row


// ------------------------------- CREATION OF THE NODES --------------------------------------------------------


// Creation of User Node

MERGE (u:User {user_id: row.`User_ID`})

WITH row

// Creation of the Transaction Node

MERGE (t:Transaction {transaction_id: row.`Transaction_ID`})
SET t.amount = toFloat(row.`Transaction_Amount`),
    t.type = row.`Transaction_Type`,
    t.timestamp = row.`Timestamp`,
    t.ip_address_flag = toInteger(row.`IP_Address_Flag`),
    t.card_type = row.`Card_Type`,
    t.card_age = toInteger(row.`Card_Age`),
    t.transaction_distance = toFloat(row.`Transaction_Distance`),
    t.authentication_method = row.`Authentication_Method`,
    t.risk_score = toFloat(row.`Risk_Score`),
    t.is_weekend = toBoolean(row.`Is_Weekend`),
    t.fraud_label = toInteger(row.`Fraud_Label`),
    t.account_balance = toFloat(row.`Account_Balance`),
    t.previous_fraudulent_activity = toInteger(row.`Previous_Fraudulent_Activity`),
    t.daily_transaction_count = toInteger(row.`Daily_Transaction_Count`),
    t.avg_transaction_amount_7d = toFloat(row.`Avg_Transaction_Amount_7d`),
    t.failed_transaction_count_7d = toInteger(row.`Failed_Transaction_Count_7d`)

WITH row

// Creation of the Merchant Node

MERGE (m:Merchant {merchant_category: row.`Merchant_Category`})

WITH row

// creation of the Device Node

MERGE (d:Device {device_type: row.`Device_Type`})

WITH row

// Creation of the Location Node

MERGE (l:Location {location: row.`Location`})

WITH row


//---------------------- CREATION OF THE RELATIONSHIPS ----------------------------------------------


// RELATION: USER ----> PERFORMED ------> TRANSACTION
MATCH (u:User {user_id: row.`User_ID`})
MATCH (t:Transaction {transaction_id: row.`Transaction_ID`})
MERGE (u)-[:PERFORMED]->(t)

WITH row

// RELATION: TRANSACTION -----> IN_CATEGORY -----> MERCHANT
MATCH (t:Transaction {transaction_id: row.`Transaction_ID`})
MATCH (m:Merchant {merchant_category: row.`Merchant_Category`})
MERGE (t)-[:IN_CATEGORY]->(m)

WITH row

// RELATION: TRANSACTION -----> EXECUTED_ON ------> DEVICE
MATCH (t:Transaction {transaction_id: row.`Transaction_ID`})
MATCH (d:Device {device_type: row.`Device_Type`})
MERGE (t)-[:EXECUTED_ON]->(d)

WITH row

// RELATION: TRANSACTION -----> LOCATED_AT ------> LOCATION
MATCH (t:Transaction {transaction_id: row.`Transaction_ID`})
MATCH (l:Location {location: row.`Location`})
MERGE (t)-[:LOCATED_AT]->(l)