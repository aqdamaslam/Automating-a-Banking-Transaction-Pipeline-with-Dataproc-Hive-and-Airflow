from pyspark.sql import SparkSession

# Initialize Spark Session with Hive Support and Specify Warehouse Directory
spark = SparkSession.builder \
    .appName("Hive Table Creation") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Check if Hive is working
spark.sql("SHOW DATABASES").show()

# Create Database
spark.sql("CREATE DATABASE IF NOT EXISTS credit_card")

# Use Database
spark.sql("USE credit_card")

# Create Table
create_table_query = """
CREATE TABLE IF NOT EXISTS transactions (
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
STORED AS PARQUET
"""

# Execute Query
spark.sql(create_table_query)

print("Hive Database and Table Created Successfully!")

# Stop Spark Session
spark.stop()
