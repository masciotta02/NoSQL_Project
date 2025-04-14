1. This query analyzes transactions connected to merchants via the IN_CATEGORY relationship and returns aggregated statistics for each merchant category. 
It calculates the total number of transactions (total) and the number of fraudulent transactions (frauds) per category, 
sorting the results in descending order of fraud occurrences.

MATCH (t:Transaction)-[:IN_CATEGORY]->(m:Merchant)
RETURN m.merchant_category, COUNT(t) AS total, 
       SUM(t.fraud_label) AS frauds
ORDER BY frauds DESC;

Output:
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

statistics: 

Time: 

- 250117 total DB Hits, where DB Hits means total number of access to databases during the execution of the query

- 106 ms --->  Execution Time
