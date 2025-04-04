import mysql.connector
import pandas as pd

# Load CSV
df = pd.read_csv("data/fraud_dataset.csv")

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
        "INSERT INTO Transactions (Transaction_ID, User_ID, Transaction_Amount, Transaction_Type, Timestamp, Account_Balance, Device_Type, Location, Merchant_Category, IP_Address_Flag, Previous_Fraudulent_Activity, Daily_Transaction_Count, Avg_Transaction_Amount_7d, Failed_Transaction_Count_7d, Card_Type,Card_Age, Transaction_Distance, Authentication_Method, Risk_Score, Is_Weekend, Fraud_Label) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (row["Transaction_ID"], row["User_ID"], row["Transaction_Amount"], row["Transaction_Type"], row["Timestamp"], row["Account_Balance"], row["Device_Type"], row["Location"], row["Merchant_Category"], row["IP_Address_Flag"], row["Previous_Fraudulent_Activity"], row["Daily_Transaction_Count"], row["Avg_Transaction_Amount_7d"], row["Failed_Transaction_Count_7d"], row["Card_Type"], row["Card_Age"], row["Transaction_Distance"], row["Authentication_Method"], row["Risk_Score"], row["Is_Weekend"], row["Fraud_Label"])
    )

conn.commit()
cursor.close()
conn.close()
