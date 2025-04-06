CREATE TABLE Transactions (
    Transaction_ID VARCHAR(50) PRIMARY KEY,               -- Unique identifier for each transaction (e.g., TXN_33553)
    User_ID VARCHAR(50),                                 -- Unique identifier for the user (e.g., USER_1834)
    Transaction_Amount DECIMAL(15, 2),                   -- Amount of money involved in the transaction (e.g., 39.79)
    Transaction_Type VARCHAR(50),                        -- Type of transaction (e.g., POS, Online, ATM)
    Timestamp DATETIME,                                  -- Date and time of the transaction (e.g., 2023-08-14 19:30:00)
    Account_Balance DECIMAL(15, 2),                      -- User's account balance before the transaction (e.g., 93213.17)
    Device_Type VARCHAR(50),                             -- Type of device used (e.g., Laptop, Mobile)
    Location VARCHAR(100),                               -- Geographical location where the transaction occurred (e.g., Sydney)
    Merchant_Category VARCHAR(50),                       -- Category of the merchant (e.g., Travel, Retail)
    IP_Address_Flag TINYINT(1),                          -- Whether the IP address was flagged as suspicious (0 or 1)
    Previous_Fraudulent_Activity INT,                    -- Number of past fraudulent activities by the user (e.g., 0)
    Daily_Transaction_Count INT,                         -- Number of transactions made by the user that day (e.g., 7)
    Avg_Transaction_Amount_7d DECIMAL(15, 2),            -- User's average transaction amount in the past 7 days (e.g., 437.63)
    Failed_Transaction_Count_7d INT,                     -- Count of failed transactions in the past 7 days (e.g., 3)
    Card_Type VARCHAR(50),                               -- Type of payment card used (e.g., Amex, Visa)
    Card_Age INT,                                        -- Age of the card in months (e.g., 65)
    Transaction_Distance DECIMAL(9, 2),                  -- Distance between the user's usual location and transaction location (e.g., 883.17 km)
    Authentication_Method VARCHAR(50),                   -- How the user authenticated (e.g., Biometric, PIN)
    Risk_Score FLOAT,                                    -- Fraud risk score computed for the transaction (e.g., 0.8494)
    Is_Weekend TINYINT(1),                               -- Whether the transaction occurred on a weekend (0 = Weekday, 1 = Weekend)
    Fraud_Label TINYINT(1)                               -- Target variable (0 = Not Fraud, 1 = Fraud)
);

--HYBRID NORMALIZATION
-- Users Table
CREATE TABLE Users (
    UserID VARCHAR(50)
);

-- Locations Table
CREATE TABLE Locations (
    LocationID INT AUTO_INCREMENT PRIMARY KEY,
    LocationName VARCHAR(100) UNIQUE
);

-- Devices Table
CREATE TABLE Devices (
    DeviceID INT AUTO_INCREMENT PRIMARY KEY,
    DeviceType VARCHAR(50) UNIQUE
);

-- Merchants Table
CREATE TABLE Merchants (
    MerchantID INT AUTO_INCREMENT PRIMARY KEY,
    MerchantCategory VARCHAR(50) UNIQUE
);

-- Risk_transaction table 
CREATE TABLE risk_transaction (
    TransactionID VARCHAR(50) PRIMARY KEY, 
    RiskScore FLOAT
);

--update transaction table
CREATE TABLE transactions_N (
    TransactionID VARCHAR(50) PRIMARY KEY,
    UserID VARCHAR(50),
    LocationID INT,
    DeviceID INT,
    MerchantID INT,
    
    Transaction_Amount DECIMAL(15, 2),                   
    Transaction_Type VARCHAR(50),                        
    Timestamp DATETIME,                                  
    Account_Balance DECIMAL(15, 2),                      
    IP_Address_Flag TINYINT(1),                          
    Previous_Fraudulent_Activity INT,                    
    Daily_Transaction_Count INT,                         
    Avg_Transaction_Amount_7d DECIMAL(15, 2),            
    Failed_Transaction_Count_7d INT,                     
    Card_Type VARCHAR(50),                               
    Card_Age INT,                                        
    Transaction_Distance DECIMAL(9, 2),                  
    Authentication_Method VARCHAR(50),                   
    Risk_Score FLOAT,                                    
    Is_Weekend TINYINT(1),                               
    Fraud_Label TINYINT(1)
    
);