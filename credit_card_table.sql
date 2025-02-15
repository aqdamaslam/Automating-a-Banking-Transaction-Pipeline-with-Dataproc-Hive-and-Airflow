CREATE DATABASE credit_card;

USE credit_card;

CREATE TABLE IF NOT EXISTS CreditCardTransactions (
    transaction_id SERIAL PRIMARY KEY,       -- Unique identifier for each transaction
    card_id VARCHAR(20) NOT NULL,            -- Card ID (e.g., unique identifier for the card)
    card_number CHAR(16) NOT NULL,           -- Full Card Number (should be encrypted or masked in real implementation)
    card_holder_name VARCHAR(100) NOT NULL,  -- Card holder's full name
    card_type VARCHAR(20) NOT NULL,          -- Card type (e.g., Visa, MasterCard, Amex)
    card_expiry VARCHAR(20) NOT NULL,               -- Expiry date of the card
    cvv_code CHAR(3) NOT NULL,               -- CVV code (must be encrypted or masked in real-world use)
    issuer_bank_name VARCHAR(100) NOT NULL,  -- Issuer bank name
    card_issuer_id INT NOT NULL,             -- Issuer bank unique identifier
    transaction_amount DOUBLE NOT NULL, -- Transaction amount
    transaction_date TIMESTAMP NOT NULL,     -- Date and time of transaction
    merchant_id VARCHAR(50) NOT NULL,        -- Merchant identifier (e.g., merchant name or ID)
    transaction_status VARCHAR(20) NOT NULL, -- Status of the transaction (e.g., completed, pending, failed)
    transaction_type VARCHAR(20) NOT NULL,   -- Type of transaction (e.g., purchase, refund, withdrawal)
    payment_method VARCHAR(20) NOT NULL,     -- Payment method (e.g., online, in-store, mobile)
    card_country VARCHAR(50) NOT NULL,       -- Country of the card holder
    billing_address VARCHAR(255),            -- Billing address associated with the card
    shipping_address VARCHAR(255),           -- Shipping address for the transaction
    fraud_flag BOOLEAN DEFAULT FALSE,        -- Fraud detection flag
    fraud_alert_sent BOOLEAN DEFAULT FALSE,  -- Whether fraud alert has been sent
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Date and time the record was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Last updated timestamp
    CONSTRAINT chk_card_number_length CHECK (LENGTH(card_number) = 16) -- Card number length validation
);

-- Add indexes for faster queries on important fields
CREATE INDEX idx_card_id ON CreditCardTransactions(card_id);
CREATE INDEX idx_transaction_date ON CreditCardTransactions(transaction_date);
CREATE INDEX idx_transaction_status ON CreditCardTransactions(transaction_status);
CREATE INDEX idx_card_type ON CreditCardTransactions(card_type);

-- Hive Query In first commit I was completely forgot partioning by year, month

DROP TABLE IF EXISTS credit_card.transactions;

CREATE TABLE IF NOT EXISTS credit_card.transactions (
    transaction_id INT,
    card_id STRING,
    card_number STRING,
    card_holder_name STRING,
    card_type STRING,
    card_expiry STRING,
    cvv_code STRING,
    issuer_bank_name STRING,
    card_issuer_id INT,
    transaction_amount DOUBLE,
    transaction_date TIMESTAMP,
    merchant_id STRING,
    transaction_status STRING,
    transaction_type STRING,
    payment_method STRING,
    card_country STRING,
    billing_address STRING,
    shipping_address STRING,
    fraud_flag BOOLEAN,
    fraud_alert_sent BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET;



