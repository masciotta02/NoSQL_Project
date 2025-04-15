

// -------------------------- ANALYSIS WITH PROFILE ----------------------------------------

PROFILE
MATCH (u:User)-[:PERFORMED]->(t:Transaction)
WITH u.user_id AS UserID, COUNT(t) AS TotalTransactions
RETURN UserID, TotalTransactions
ORDER BY TotalTransactions DESC
LIMIT 10;

/* 
We can use PROFILE that allows us to execute a query while providing detailed performance metrics, such as the number of rows processed (Rows), database accesses (DbHits) and memory usage at each step of the execution plan. This helps identify bottlenecks and optimize the query by highlighting expensive operations.


## OUTPUT:

An execution plan that it has been visualized like a tree made of logic operators, 
in particular each node represents a specific phase of the query's execution with computed metrics associated:

# 1. NodeByLabelScan:

   This phase represents a complete scan of the Transaction node,
   the operator "NodeBylabelScan" denote that there aren't indexes or costraints that should optimize the query.

-----------------------------------
 |  METRICS:                       |
 |   - Memory (bytes) : 376 byte   |
 |   - Pagecache hits : 58.828     |
 |   - Pagecache misses : 0        |
 |   - Estimated rows : 8.963      |
 |   - DbHits : 8964               |
 |   - Rows : 8963                 |
 -----------------------------------

# 2. Expand(All):

   In this phase we have that Neo4j proceed to expand the relationships "PERFORMED" to the User node, this means that it will find, for each transaction, all the users that made that transaction. The result that we will have is a set of tuple (t,u) that represent all the relations "PERFORMED".

  ---------------------------------
  | METRICS:                      |
  |  - Estimated rows : 50.000    |
  |  - DbHits : 58963             |
  |  -------------------------------

# 3. FILTER:

   This is a check made by Neo4j to be sure that all nodes with relation "PERFORMED" are all User type.

 -----------------------------------
 |  METRICS:                       |
 |   - Estimated rows : 50.000     |
 |   - DbHits : 100.000            |
 -----------------------------------


# 4. EagerAggregation:

   This phase represents to group precedent results based on the fiel "user_id", then it will compute the COUNT() of transactions. The grouping is called eager beacuse Neo4j execute the grouping, the count and only after this it will proceed with other operations.

    -----------------------------------
 |  METRICS:                        |
 |   - Memory (bytes) :  1,510,856  |
 |   - Estimated rows : 224         |
 |   - DbHits : 0                   |
 -----------------------------------

# 5. Top:

   In this passage, Neo4j sorts the results based on Total_Transactions in descendent order, then it selects the first 10 results.

   -------------------------------------
 |  METRICS:                           |
 |   - Memory (bytes) : 590,680 byte   |
 |   - Rows : 10                       |
 |   - DbHits : 0                      |
 ---------------------------------------

# 6. ProduceResults:

   The last phase is for produce the final results based on what we want from the query, in this case user_id, Total_transactions. 

-------------------------------------------
 |  METRICS:                              |
 |   - Total Memory (bytes) : 1,908,768   |
 |   - DbHits : 0                         |
 |   - Rows : 10                          |
 ------------------------------------------





 ------------------- Potential Optimizations with Indexes ------------------------------


To improve the performance of this query, we can consider the following optimizations using indexes:

# 1. Index on Transaction Label:

If there are many nodes in the database, scanning all Transaction nodes can be expensive. An index on the Transaction label can speed up the initial scan: 
*/

 CREATE INDEX FOR (t:Transaction) ON (t.transaction_id); 


// 2. Index on User(user_id):

// The query aggregates data based on user_id. Adding an index on user_id ensures that Neo4j can quickly locate and group users by their IDs:

 CREATE INDEX FOR (u:User) ON (u.user_id); 


// 3. Constraint on User(user_id):

//If user_id is unique for each user, defining a uniqueness constraint can help Neo4j manage queries based on user_id more efficiently:

 CREATE CONSTRAINT user_id_unique FOR (u:User) REQUIRE u.user_id IS UNIQUE;


// -------------------------- Query Analysis with Indexes --------------------------


PROFILE
MATCH (u:User)-[:PERFORMED]->(t:Transaction)
WITH u.user_id AS UserID, COUNT(t) AS TotalTransactions
RETURN UserID, TotalTransactions
ORDER BY TotalTransactions DESC
LIMIT 10;


/*
1. NodeIndexScan:

      -----------------------------------
      |  METRICS:                       |
      |   - Memory (bytes) : 376 byte   |
      |   - Pagecache hits : 108.740    |
      |   - Pagecache misses : 0        |
      |   - Estimated rows : 8.963      |
      |   - DbHits : 8964               |
      |   - Rows : 8963                 |
      -----------------------------------

# 2. ExpandAll:

      ---------------------------------
      | METRICS:                      |
      |  - Estimated rows : 50.000    |
      |  - DbHits : 58963             |
      |--------------------------------

# 3. Filter:

      -----------------------------------
      |  METRICS:                       |
      |   - Estimated rows : 50.000     |
      |   - DbHits : 100.000            |
      -----------------------------------

# 4. OrderedAggregation:


      -----------------------------------
      |  METRICS:                        |
      |   - Memory (bytes) :  94.712     |    # Impactive Improvement!
      |   - Estimated rows : 224         |
      |   - DbHits : 0                   |
      -----------------------------------

# 5. Top:

      -------------------------------------
      |  METRICS:                           |
      |   - Memory (bytes) : 183,384 byte   | # Impactive Improvement!
      |   - Rows : 10                       |
      |   - DbHits : 0                      |
      ---------------------------------------

# 6. ProduceResults:

      -------------------------------------------
      |  METRICS:                              |
      |   - Total Memory (bytes) : 278,552     | # Impactive Improvement!
      |   - DbHits : 0                         |
      |   - Rows : 10                          |
      ------------------------------------------


# Conclusion

The analysis of the base query and the optimized query highlights the significant impact of using indexes in Neo4j. 
By forcing the use of indexes on User(user_id) and Transaction(transaction_id), the optimized query reduces memory usage and database hits, 
leading to improved performance. For instance, operations like OrderedAggregation and Top show a marked decrease in memory consumption (like 94.712 bytes for aggregation and 183,384 bytes for sorting),
with zero database hits (DbHits). This indicates that all necessary data was efficiently retrieved during earlier stages, minimizing redundant access. 
The final ProduceResults phase also reflects this efficiency, with total memory usage capped at 278,552 bytes (base query: 1,908,768 bytes). */




//Put this beacuse .cypher file can't end with a comment, 
//I want to recommend that It's useless for the code above.

MATCH(p:prova) return p; 

