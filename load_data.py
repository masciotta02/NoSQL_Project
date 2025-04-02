import mysql.connector
import pandas as pd

# Load CSV
df = pd.read_csv("data/transaction_data.csv")

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="biar",
    database="banking_db"
)
cursor = conn.cursor()

# Insert Data
for _, row in df.iterrows():
    cursor.execute(
        "INSERT INTO transactions (Transaction_ID, Sender_Account_ID, Receiver_Account_ID, Transaction_Amount, Transaction_Type, Timestamp, Transaction_Status, Fraud_Flag, Geolocation, Device_Used, Network_Slice_ID, Latency_ms, Slice_Bandwidth_Mbps, PIN_Code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (row["Transaction ID"], row["Sender Account ID"], row["Receiver Account ID"], row["Transaction Amount"], row["Transaction Type"], row["Timestamp"], row["Transaction Status"], row["Fraud Flag"], row["Geolocation (Latitude/Longitude)"], row["Device Used"], row["Network Slice ID"], row["Latency (ms)"], row["Slice Bandwidth (Mbps)"], row["PIN Code"])
    )

conn.commit()
cursor.close()
conn.close()
