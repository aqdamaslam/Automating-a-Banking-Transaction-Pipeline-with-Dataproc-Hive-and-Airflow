import pandas as pd
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker for realistic fake data
fake = Faker()

# Define possible values for categorical fields
card_types = ["Visa", "MasterCard", "Amex", "Discover"]
transaction_statuses = ["Completed", "Pending", "Failed"]
transaction_types = ["Purchase", "Refund", "Withdrawal"]
payment_methods = ["Online", "In-Store", "Mobile"]
countries = ["USA", "UK", "Canada", "Australia", "India", "Germany", "France", "Brazil", "Japan", "UAE"]
merchant_ids = [str(uuid.uuid4())[:50] for _ in range(10)]  # Example list of merchant IDs
bank_name = "Chase"  # Fixed bank name


# Function to generate a single month's worth of data
def generate_monthly_data(month, year, num_records=10000):
    data = []
    transaction_id = (month - 1) * num_records + 1  # Start ID for the month
    
    for _ in range(num_records):
        card_id = str(uuid.uuid4())[:20]  # Unique card ID
        card_number = "".join([str(random.randint(0, 9)) for _ in range(16)])
        card_holder_name = fake.name()
        card_type = random.choice(card_types)
        card_expiry = fake.date_between(start_date="+1y", end_date="+5y")  # Future expiry
        cvv_code = str(random.randint(100, 999))
        card_issuer_id = random.randint(1000, 9999)
        transaction_amount = round(random.uniform(1, 5000), 2)  # Random amount between 1 and 5000
        transaction_date = datetime(year, month, random.randint(1, 28)) + timedelta(hours=random.randint(0, 23),
                                                                                     minutes=random.randint(0, 59),
                                                                                     seconds=random.randint(0, 59))
        # Format transaction date
        transaction_date_str = transaction_date.strftime("%Y-%m-%d %H:%M:%S")
        
        merchant_id = random.choice(merchant_ids)  # Random merchant ID
        transaction_status = random.choice(transaction_statuses)
        transaction_type = random.choice(transaction_types)
        payment_method = random.choice(payment_methods)
        card_country = random.choice(countries)
        billing_address = fake.address().replace("\n", ", ")
        shipping_address = fake.address().replace("\n", ", ") if random.random() > 0.5 else None
        fraud_flag = random.random() < 0.05  # 5% fraud cases
        fraud_alert_sent = fraud_flag and (random.random() < 0.7)  # 70% of fraud cases sent alert
        
        # Create and format created_at and updated_at
        created_at = transaction_date
        created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")
        updated_at = transaction_date + timedelta(days=random.randint(0, 30))
        updated_at_str = updated_at.strftime("%Y-%m-%d %H:%M:%S")
        
        data.append([
            transaction_id, card_id, card_number, card_holder_name, card_type, card_expiry, cvv_code, bank_name,
            card_issuer_id, transaction_amount, transaction_date_str, merchant_id, transaction_status,
            transaction_type, payment_method, card_country, billing_address, shipping_address,
            fraud_flag, fraud_alert_sent, created_at_str, updated_at_str
        ])

        transaction_id += 1  # Increment transaction ID for next record
    
    return data

# Generate data for 12 months
all_data = []
for month in range(1, 13):  # For each month (1-12)
    month_data = generate_monthly_data(month, 2025)  # Changed year to 2025
    all_data.extend(month_data)

# Create DataFrame
columns = [
    "transaction_id", "card_id", "card_number", "card_holder_name", "card_type", "card_expiry", "cvv_code",
    "issuer_bank_name", "card_issuer_id", "transaction_amount", "transaction_date", "merchant_id",
    "transaction_status", "transaction_type", "payment_method", "card_country", "billing_address",
    "shipping_address", "fraud_flag", "fraud_alert_sent", "created_at", "updated_at"
]

df = pd.DataFrame(all_data, columns=columns)

# Save as CSV file
csv_filename = "CreditCardTransactions_2025.csv"
df.to_csv(csv_filename, index=False)

csv_filename
