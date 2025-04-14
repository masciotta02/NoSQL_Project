-- Query performance analysis
EXPLAIN SELECT 
    UserID, 
    COUNT(TransactionID) AS TotalTransactions
FROM 
    transactions_N
GROUP BY 
    UserID
ORDER BY 
    TotalTransactions DESC
LIMIT 10;

--OUTPUT
+----+-------------+----------------+------------+------+---------------+------+---------+------+-------+----------+---------------------------------+
| id | select_type | table          | partitions | type | possible_keys | key  | key_len | ref  | rows  | filtered | Extra                           |
+----+-------------+----------------+------------+------+---------------+------+---------+------+-------+----------+---------------------------------+
|  1 | SIMPLE      | transactions_N | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 49764 |   100.00 | Using temporary; Using filesort |
+----+-------------+----------------+------------+------+---------------+------+---------+------+-------+----------+---------------------------------+

-- with EXPLAIN ANALYZE
--OUTPUT
| -> Limit: 10 row(s)  (actual time=175..175 rows=10 loops=1)
    -> Sort: TotalTransactions, limit input to 10 row(s) per chunk  (actual time=175..175 rows=10 loops=1)
        -> Table scan on <temporary>  (actual time=171..173 rows=8963 loops=1)
            -> Aggregate using temporary table  (actual time=171..171 rows=8963 loops=1)
                -> Table scan on transactions_N  (cost=5457 rows=49764) (actual time=1.85..93 rows=50000 loops=1)
 |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.18 sec)

--with INDEX (improvement)
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| -> Limit: 10 row(s)  (actual time=54.3..54.3 rows=10 loops=1)
    -> Sort: TotalTransactions, limit input to 10 row(s) per chunk  (actual time=54.3..54.3 rows=10 loops=1)
        -> Stream results  (cost=10073 rows=9055) (actual time=0.2..51.7 rows=8963 loops=1)
            -> Group aggregate: count(transactions_N.TransactionID)  (cost=10073 rows=9055) (actual time=0.193..48.1 rows=8963 loops=1)
                -> Covering index scan on transactions_N using idx_user_id  (cost=5097 rows=49764) (actual time=0.127..28 rows=50000 loops=1)
 |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.06 sec)

--Resource Usage Analysis
-- Enable profiling
SET profiling = 1;

-- Run your query
SELECT 
    UserID, 
    COUNT(TransactionID) AS TotalTransactions
FROM 
    transactions_N
GROUP BY 
    UserID
ORDER BY 
    TotalTransactions DESC
LIMIT 10;

-- View profiling results
SHOW PROFILES;
SHOW PROFILE FOR QUERY 1; -- Replace '1' with the query ID from SHOW PROFILES

+--------------------------------+----------+
| Status                         | Duration |
+--------------------------------+----------+
| starting                       | 0.000354 |
| Executing hook on transaction  | 0.000012 |
| starting                       | 0.000013 |
| checking permissions           | 0.000010 |
| Opening tables                 | 0.000177 |
| init                           | 0.000020 |
| System lock                    | 0.000018 |
| optimizing                     | 0.000009 |
| statistics                     | 0.000028 |
| preparing                      | 0.000018 |
| Creating tmp table             | 0.000087 |
| executing                      | 0.049173 |
| end                            | 0.000032 |
| query end                      | 0.000009 |
| waiting for handler commit     | 0.000027 |
| closing tables                 | 0.000013 |
| freeing items                  | 0.000191 |
| cleaning up                    | 0.000026 |
+--------------------------------+----------+

--Monitor resource usage
SHOW STATUS LIKE 'Handler_read%'; -- Index usage statistics
+-----------------------+--------+
| Variable_name         | Value  |
+-----------------------+--------+
| Handler_read_first    | 11     |
| Handler_read_key      | 27     |
| Handler_read_last     | 0      |
| Handler_read_next     | 350009 |
| Handler_read_prev     | 0      |
| Handler_read_rnd      | 0      |
| Handler_read_rnd_next | 50036  |
+-----------------------+--------+
7 rows in set (0.03 sec)
SHOW STATUS LIKE 'Created_tmp%'; -- Temporary table usage
+-------------------------+-------+
| Variable_name           | Value |
+-------------------------+-------+
| Created_tmp_disk_tables | 0     |
| Created_tmp_files       | 13    |
| Created_tmp_tables      | 5     |
+-------------------------+-------+
3 rows in set (0.00 sec)
SHOW STATUS LIKE 'Sort%'; -- Sorting statistics
+-------------------+-------+
| Variable_name     | Value |
+-------------------+-------+
| Sort_merge_passes | 0     |
| Sort_range        | 0     |
| Sort_rows         | 82    |
| Sort_scan         | 9     |
+-------------------+-------+
4 rows in set (0.00 sec)

--Space complexity analysis
SELECT 
    table_name AS `Table`, 
    ROUND((data_length + index_length) / 1024 / 1024, 2) AS `Size (MB)` 
FROM 
    information_schema.tables 
WHERE 
    table_schema = 'banking_db';

+------------------+-----------+
| Table            | Size (MB) |
+------------------+-----------+
| Devices          |      0.03 |
| Locations        |      0.03 |
| Merchants        |      0.03 |
| Transactions     |     12.52 |
| Users            |      0.36 |
| risk_transaction |      1.52 |
| transactions_N   |      7.52 |
+------------------+-----------+
7 rows in set (0.04 sec)


