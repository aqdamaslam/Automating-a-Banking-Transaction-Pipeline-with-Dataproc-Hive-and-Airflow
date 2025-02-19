import subprocess
import sys

# Install required packages
required_packages = ["pymysql", "cloud-sql-python-connector", "pyspark", "pandas"]
for package in required_packages:
    subprocess.run([sys.executable, "-m", "pip", "install", package, "--break-system-packages"], check=True)

import os
import pymysql
from google.cloud.sql.connector import Connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Load environment variables for security
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME", "[YOUR-GCP-INSTANCE-CONNECTION NAME]")
DB_USER = os.getenv("DB_USER", "YOUR-GCP-MYSQL-USERNAME")
DB_PASS = os.getenv("DB_PASS", "[YOUR-GCP-MYSQL-USERNAME]")
DB_NAME = os.getenv("DB_NAME", "[YOUR-GCP-MYSQL-DATABASE-NAME]")

def fetch_data_from_gcp_sql():
    """ Connects to Google Cloud SQL using the Cloud SQL Connector and fetches transaction records."""
    connector = Connector(ip_type="PUBLIC")

    try:
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pymysql",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
        )
        print("Connection successful!")

        cursor = conn.cursor()

        query = """
        SELECT * 
        FROM CreditCardTransactions
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
        AND transaction_date < CURRENT_DATE()
        """
        cursor.execute(query)
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        return records
    
    except Exception as e:
        print(f"Error: {e}")
        return []

def load_data_to_dataproc_hive(records):
    """Loads transaction data into a Hive table on Google Cloud Dataproc."""
    spark = SparkSession.builder \
        .appName("GCP SQL to Hive Parquet with Partitioning") \
        .enableHiveSupport() \
        .getOrCreate()

    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("card_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("card_holder_name", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("card_expiry", StringType(), True),
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
        StructField("fraud_flag", StringType(), True),
        StructField("fraud_alert_sent", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    rdd = spark.sparkContext.parallelize(records)
    df = spark.createDataFrame(rdd, schema)
    df = df.withColumn("year", year(df["transaction_date"])) \
           .withColumn("month", month(df["transaction_date"]))
    
    hive_database = "YOUR-HIVE-DATABASE"
    hive_table = "[YOUR-HIVE-TABLE]"

    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("parquet.compression", "SNAPPY") \
        .partitionBy("year", "month") \
        .saveAsTable(f"{hive_database}.{hive_table}")

    print("Data successfully loaded into Hive table with year/month partitioning in Parquet format!")

def main():
    records = fetch_data_from_gcp_sql()
    if records:
        load_data_to_dataproc_hive(records)

if __name__ == "__main__":
    main()
