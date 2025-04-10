
// CREAZIONE DEI NODI


// NODO USER


LOAD CSV WITH HEADERS FROM 'file:///synthetic_fraud_dataset.csv' AS row
MERGE (u:User {user_id: row.`User_ID`})
SET u.account_balance = toFloat(row.`Account_Balance`),
    u.previous_fraudulent_activity = toInteger(row.`Previous_Fraudulent_Activity`),
    u.daily_transaction_count = toInteger(row.`Daily_Transaction_Count`),
    u.avg_transaction_amount_7d = toFloat(row.`Avg_Transaction_Amount_7d`),
    u.failed_transaction_count_7d = toInteger(row.`Failed_Transaction_Count_7d`) 



// NODO PER LE TRANSACTION


LOAD CSV WITH HEADERS FROM 'file:///synthetic_fraud_dataset.csv' AS row
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
    t.fraud_label = toInteger(row.`Fraud_Label`)


// NODO PER I MERCHANT

LOAD CSV WITH HEADERS FROM 'file:///synthetic_fraud_dataset.csv' AS row
MERGE (m:Merchant {merchant_category: row.`Merchant_Category`})



// NODO PER I DEVICE


LOAD CSV WITH HEADERS FROM 'file:///synthetic_fraud_dataset.csv' AS row
MERGE (d:Device {device_type: row.`Device_Type`})


// NODO PER LA LOCATION

LOAD CSV WITH HEADERS FROM 'file:///synthetic_fraud_dataset.csv' AS row
MERGE (l:Location {location: row.`Location`})



// ----------- CREAZIONE DELLE RELAZIONI ------------------



// RELAZIONE: USER----> PERFORMED ------> TRANSACTION


LOAD CSV WITH HEADERS FROM 'file:///synthetic_fraud_dataset.csv' AS row
MATCH (u:User {user_id: row.`User ID`})
MATCH (t:Transaction {transaction_id: row.`Transaction ID`})
MERGE (u)-[:PERFORMED]->(t)


// RELAZIONE: TRANSACTION -----> IN_CATEGORY -----> MERCHANT


LOAD CSV WITH HEADERS FROM 'file:///synthetic_fraud_dataset.csv' AS row
MATCH (t:Transaction {transaction_id: row.`Transaction_ID`})
MATCH(m:Merchant {merchant_category: row.`Merchant_Category`})
MERGE (t)-[:IN_CATEGORY]->(m)


// RELAZIONE: TRANSACTION -----> EXECUTED ON ------> DEVICE


LOAD CSV WITH HEADERS FROM 'file:///synthetic_fraud_dataset.csv' AS row
MATCH (t:Transaction {transaction_id: row.`Transaction ID`})
MATCH (d:Device {device_type: row.`Device Type`})
MERGE (t)-[:EXECUTED_ON]->(d)



// RELAZIONE: TRANSACTION -----> LOCATED_AT ------> LOCATION


LOAD CSV WITH HEADERS FROM 'file:///synthetic_fraud_dataset.csv' AS row
MATCH (t:Transaction {transaction_id: row.`Transaction ID`})
MATCH (l:Location {location: row.`Location`})
MERGE (t)-[:LOCATED_AT]->(l)