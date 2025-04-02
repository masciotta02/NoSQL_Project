
// CREAZIONE DEI NODI


// NODI PER ACCOUNT SENDER E ACCOUNT RECEIVER


LOAD CSV WITH HEADERS FROM 'file:///transaction_data.csv' AS row
MERGE (s:AccountSender {id: row.`Sender Account ID`});

LOAD CSV WITH HEADERS FROM 'file:///transaction_data.csv' AS row
MERGE (r:AccountReceiver {id: row.`Receiver Account ID`});


// NODO PER LE TRANSACTION


LOAD CSV WITH HEADERS FROM 'file:///transaction_data.csv' AS row
MERGE (t:Transaction {
    id: row.`Transaction ID`,
    amount: toFloat(row.`Transaction Amount`),
    type: row.`Transaction Type`,
    timestamp: row.`Timestamp`,
    status: row.`Transaction Status`,
    fraud: row.`Fraud Flag`,
    pin_code: row.`PIN Code`
});


// NODO PER I DEVICE


LOAD CSV WITH HEADERS FROM 'file:///transaction_data.csv' AS row
MERGE (d:Device {
    type: row.`Device Used`,
    network_slice_id: row.`Network Slice ID`,
    latency: toFloat(row.`Latency (ms)`),
    bandwidth: toFloat(row.`Slice Bandwidth (Mbps)`)
});


// NODO PER LA GEOLOCALIZZAZIONE DELLE TRANSACTION


LOAD CSV WITH HEADERS FROM 'file:///transaction_data.csv' AS row
MERGE (l:Location {
    geolocation: row.`Geolocation (Latitude/Longitude)`
});



// ----------- CREAZIONE DELLE RELAZIONI ------------------



// RELAZIONE: ACCOUNT SENDER ----> SENT ------> TRANSACTION


LOAD CSV WITH HEADERS FROM 'file:///transaction_data.csv' AS row
MATCH (s:AccountSender {id: row.`Sender Account ID`})
MATCH (t:Transaction {id: row.`Transaction ID`})
MERGE (s)-[:SENT]->(t);


// RELAZIONE: ACCOUNT RECEIVER ----> RECEIVED ------> TRANSACTION


LOAD CSV WITH HEADERS FROM 'file:///transaction_data.csv' AS row
MATCH (r:AccountReceiver {id: row.`Receiver Account ID`})
MATCH (t:Transaction {id: row.`Transaction ID`})
MERGE (r)-[:RECEIVED]->(t);


// RELAZIONE: TRANSACTION -----> EXECUTED ON ------> DEVICE


LOAD CSV WITH HEADERS FROM 'file:///transaction_data.csv' AS row
MATCH (t:Transaction {id: row.`Transaction ID`})
MATCH (d:Device {type: row.`Device Used`})
MERGE (t)-[:EXECUTED_ON]->(d);


// RELAZIONE: TRANSACTION -----> GEOLOCATED_AT ------> LOCATION


LOAD CSV WITH HEADERS FROM 'file:///transaction_data.csv' AS row
MATCH (t:Transaction {id: row.`Transaction ID`})
MATCH (l:Location {geolocation: row.`Geolocation (Latitude/Longitude)`})
MERGE (t)-[:GEOLOCATED_AT]->(l);
