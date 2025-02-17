import pymysql
from google.cloud.sql.connector import Connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


""" Connects to Google Cloud SQL using the Cloud SQL Connector.
    Fetches transaction records from the `CreditCardTransactions` table for the past month. 
"""

def fetch_data_from_gcp_sql():
    instance_connection_name = '[YOUR-INSTANCE-CONNECTION-NAME]'
    db_user = '[DB-USERS]'
    db_pass = '[DB-PASSWORD]'
    db_name = '[DB-NAME]'

    # Set up connector
    connector = Connector(ip_type="PUBLIC")

    try:
        conn = connector.connect(
            instance_connection_name,
            "pymysql",
            user=db_user,
            password=db_pass,
            db=db_name,
        )
        print("Connection successful!")

        cursor = conn.cursor()

 # Query to fetch transactions from the last month

        query = """
        SELECT * 
        FROM [YOU-SQL-TABLE-NAME]
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
        AND transaction_date < CURRENT_DATE() # Use LIMIT 10000 in query if your Spark cluster has memory issues
        """
        cursor.execute(query)
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()

        return records
    
    except Exception as e:
        print(f"Error: {e}")
        return []

""" Loads the fetched transaction data into a Hive table on Google Cloud Dataproc.
    The table is stored in Parquet format and partitioned by year and month. """

def load_data_to_dataproc_hive(records):
    # Create a Spark session with Hive support enabled
    spark = SparkSession.builder \
        .appName("GCP SQL to Hive Parquet with Partitioning") \
        .enableHiveSupport() \
        .getOrCreate()

    # Define the schema for the DataFrame (must match with the SQL database table structure)
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

    # Convert raw records into an RDD and then to a DataFrame
    rdd = spark.sparkContext.parallelize(records)
    df = spark.createDataFrame(rdd, schema)

    # Extract year and month from `transaction_date` to use as partitions
    df = df.withColumn("year", year(df["transaction_date"])) \
           .withColumn("month", month(df["transaction_date"]))
    
    # Define Hive database and table
    hive_database = "[YOUR-HIVE-DATABASE]"
    hive_table = "[YOUR-HIVE-TABLE]"

    # Enable dynamic partitioning
    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

    # Write the DataFrame to Hive with partitioning by year and month
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("parquet.compression", "SNAPPY") \
        .partitionBy("year", "month") \
        .saveAsTable(f"{hive_database}.{hive_table}")

    print("Data successfully loaded into Hive table with year/month partitioning in Parquet format!")
    
# Main function that fetches data from GCP SQL and loads it into Hive. 

def main():
    records = fetch_data_from_gcp_sql()
    if records:
        load_data_to_dataproc_hive(records)

if __name__ == "__main__":
    main()
