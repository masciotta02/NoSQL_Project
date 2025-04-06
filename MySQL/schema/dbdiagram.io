--Script for dbdiagram.io
Table Users {
    UserID VARCHAR(50) [pk]
}

Table Locations {
    LocationID INT [pk, increment]
    LocationName VARCHAR(100) [unique]
}

Table Devices {
    DeviceID INT [pk, increment]
    DeviceType VARCHAR(50) [unique]
}

Table Merchants {
    MerchantID INT [pk, increment]
    MerchantCategory VARCHAR(50) [unique]
}

Table Transactions_N {
    TransactionID VARCHAR(50) [pk]
    UserID VARCHAR(50) [ref: > Users.UserID]
    LocationID INT [ref: > Locations.LocationID]
    DeviceID INT [ref: > Devices.DeviceID]
    MerchantID INT [ref: > Merchants.MerchantID]
    Transaction_Amount DECIMAL(15, 2)
    Transaction_Type VARCHAR(50)
    Timestamp DATETIME
    Account_Balance DECIMAL(15, 2)
    IP_Address_Flag TINYINT(1)
    Previous_Fraudulent_Activity INT
    Daily_Transaction_Count INT
    Avg_Transaction_Amount_7d DECIMAL(15, 2)
    Failed_Transaction_Count_7d INT
    Card_Type VARCHAR(50)
    Card_Age INT
    Transaction_Distance DECIMAL(9, 2)
    Authentication_Method VARCHAR(50)
    Risk_Score FLOAT
    Is_Weekend TINYINT(1)
    Fraud_Label TINYINT(1)
}
