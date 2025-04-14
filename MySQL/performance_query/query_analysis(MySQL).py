import mysql.connector
import time
import csv

# Step 1: Connect to the MySQL database
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host="localhost",         # Replace with your host
            user="root",     # Replace with your username
            password="biar", # Replace with your password
            database="banking_db"     # Replace with your database name
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

# Step 2: Execute a query and measure execution time
def execute_query_and_measure_time(connection, query):
    cursor = connection.cursor()
    start_time = time.time()  # Start timing
    cursor.execute(query)
    result = cursor.fetchall()  # Fetch all results (if needed)
    end_time = time.time()      # End timing
    cursor.close()
    execution_time = end_time - start_time
    return execution_time

# Step 3: Save execution time to a CSV file
def save_to_csv(filename, query_name, execution_time):
    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([query_name, execution_time])

# Step 4: Main function to run the process
def main():
    # Define your query
    query_name = "Query_1"
    query = """
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
    """

    # Connect to the database
    connection = connect_to_db()
    if connection is None:
        return

    # Measure execution time
    execution_time = execute_query_and_measure_time(connection, query)

    # Save execution time to a CSV file
    filename = "query_execution_times.csv"
    save_to_csv(filename, query_name, execution_time)

    # Close the database connection
    connection.close()

    print(f"Execution time for '{query_name}': {execution_time:.4f} seconds")

if __name__ == "__main__":
    main()