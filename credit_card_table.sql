create database credit_card;

use credit_card;

CREATE TABLE CreditCardTransactions (
    transaction_id SERIAL PRIMARY KEY,       -- Unique identifier for each transaction
    card_id VARCHAR(20) NOT NULL,            -- Card ID (e.g., unique identifier for the card)
    card_number CHAR(16) NOT NULL,           -- Full Card Number (should be encrypted or masked in real implementation)
    card_holder_name VARCHAR(100) NOT NULL,  -- Card holder's full name
    card_type VARCHAR(20) NOT NULL,          -- Card type (e.g., Visa, MasterCard, Amex)
    card_expiry DATE NOT NULL,               -- Expiry date of the card
    cvv_code CHAR(3) NOT NULL,               -- CVV code (must be encrypted or masked in real-world use)
    issuer_bank_name VARCHAR(100) NOT NULL,  -- Issuer bank name
    card_issuer_id INT NOT NULL,             -- Issuer bank unique identifier
    transaction_amount DECIMAL(10, 2) NOT NULL, -- Transaction amount
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

