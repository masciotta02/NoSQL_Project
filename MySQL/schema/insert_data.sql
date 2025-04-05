--Populate normalized tables from existing data
-- Populate Users
INSERT INTO Users (UserID)
SELECT DISTINCT User_ID
FROM Transactions;

-- Populate Locations
INSERT INTO Locations (LocationName)
SELECT DISTINCT Location FROM Transactions;

-- Populate Devices
INSERT INTO Devices (DeviceType)
SELECT DISTINCT Device_Type FROM Transactions;

-- Populate Merchants
INSERT INTO Merchants (MerchantCategory)
SELECT DISTINCT Merchant_Category FROM Transactions;

--Populate risk_transaction
INSERT INTO risk_transaction (TransactionID,RiskScore)
SELECT DISTINCT Transaction_ID,Risk_Score FROM Transactions WHERE Risk_Score >= 0.7;

--Populate transactions_N
INSERT INTO transactions_N (
    TransactionID, UserID, LocationID, DeviceID, MerchantID,
    Transaction_Amount, Transaction_Type, Timestamp, Account_Balance,
    IP_Address_Flag, Previous_Fraudulent_Activity, Daily_Transaction_Count,
    Avg_Transaction_Amount_7d, Failed_Transaction_Count_7d, Card_Type,
    Card_Age, Transaction_Distance, Authentication_Method, Risk_Score,
    Is_Weekend, Fraud_Label
)
SELECT 
    t.Transaction_ID, t.User_ID,
    l.LocationID, d.DeviceID, m.MerchantID,
    t.Transaction_Amount, t.Transaction_Type, t.Timestamp, t.Account_Balance,
    t.IP_Address_Flag, t.Previous_Fraudulent_Activity, t.Daily_Transaction_Count,
    t.Avg_Transaction_Amount_7d, t.Failed_Transaction_Count_7d, t.Card_Type,
    t.Card_Age, t.Transaction_Distance, t.Authentication_Method, t.Risk_Score,
    t.Is_Weekend, t.Fraud_Label
FROM 
    Transactions t
LEFT JOIN Locations l ON t.Location = l.LocationName
LEFT JOIN Devices d ON t.Device_Type = d.DeviceType
LEFT JOIN Merchants m ON t.Merchant_Category = m.MerchantCategory;
