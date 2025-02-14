import pymysql
from google.cloud.sql.connector import Connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def fetch_data_from_gcp_sql():
    instance_connection_name = '[YOUR-INSTANCE-CONNECTION-NAME]'
    db_user = '[DB-USERS]'
    db_pass = '[DB-PASSWORD]'
    db_name = '[DB-NAME]'

    # Set up connector
    connector = Connector(ip_type="PUBLIC")

    # Test connection and fetch records using pymysql
    try:
        conn = connector.connect(
            instance_connection_name,
            "pymysql",
            user=db_user,
            password=db_pass,
            db=db_name,
        )
        print("Connection successful!")

        # Create a cursor to execute SQL queries
        cursor = conn.cursor()

        # Execute query to fetch the first 10 records from a specific table
        query = """
    SELECT * 
    FROM CreditCardTransactions
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
    AND transaction_date < CURRENT_DATE() # Use LIMIT 10000 in query if your spark cluster memory isuue
"""
        cursor.execute(query)

        # Fetch the results
        records = cursor.fetchall()
        
        # Close the connection
        cursor.close()
        conn.close()

        return records
    
    except Exception as e:
        print(f"Error: {e}")
        return []

def load_data_to_dataproc_hive(records):
    # Create a Spark session to connect to Dataproc
    spark = SparkSession.builder \
        .appName("GCP SQL to Hive Parquet") \
        .enableHiveSupport() \
        .getOrCreate()

    # Define the schema for the data based on the SQL query structure
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("card_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("card_holder_name", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("card_expiry", StringType(), True),  # You can adjust type as per actual format
        StructField("cvv_code", StringType(), True),
        StructField("issuer_bank_name", StringType(), True),
        StructField("card_issuer_id", IntegerType(), True),
        StructField("transaction_amount", DoubleType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("card_country", StringType(), True),
        StructField("billing_address", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("fraud_flag", StringType(), True),  # Adjust type as per actual format
        StructField("fraud_alert_sent", StringType(), True),  # Adjust type as per actual format
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    # Create a DataFrame from the fetched records
    rdd = spark.sparkContext.parallelize(records)
    df = spark.createDataFrame(rdd, schema)

    # Load data into Hive table in Parquet format (overwrite the existing table)
    hive_database = "credit_card"  # Change this to your target Hive database
    hive_table = "transactions"  # Change this to your target Hive table

    # Write data to Hive in Parquet format and overwrite the table
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("parquet.compression", "SNAPPY") \
        .saveAsTable(f"{hive_database}.{hive_table}")

    print("Data successfully loaded into Hive table in Parquet format (overwritten)!")


def main():
    # Step 1: Fetch data from GCP SQL
    records = fetch_data_from_gcp_sql()
    
    if records:
        # Step 2: Load data into Dataproc Hive table in Parquet format
        load_data_to_dataproc_hive(records)

if __name__ == "__main__":
    main()
