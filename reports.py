from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, year, month, lit
from datetime import datetime

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Last Month Credit Card Usage Report") \
    .enableHiveSupport() \
    .getOrCreate()

# Increase max fields displayed
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Define Hive database and table
hive_database = "credit_card"
hive_table = "transactions"

# Get the current date
current_date = datetime.now()

# Determine last month and year (handle year change from Jan → Dec)
if current_date.month == 1:
    last_month = 12
    last_year = current_date.year - 1
else:
    last_month = current_date.month - 1
    last_year = current_date.year

# Read data from Hive and filter for last month’s transactions
df = spark.sql(f"""
    SELECT * FROM {hive_database}.{hive_table}
    WHERE year(transaction_date) = {last_year} 
    AND month(transaction_date) = {last_month}
""")

# If no data is available for the last month, exit the script
if df.rdd.isEmpty():
    print("No data found for the last month. Exiting...")
    exit()

# Aggregate user-wise monthly spending
usage_df = df.groupBy("card_holder_name") \
    .agg(
        sum("transaction_amount").alias("total_spent"),
        count("transaction_id").alias("transaction_count"),
        avg("transaction_amount").alias("avg_transaction_amount")
    )

# Join aggregated values with transaction details
df = df.join(usage_df, on="card_holder_name", how="left")

# Add year and month columns for partitioning
df = df.withColumn("year", lit(last_year)).withColumn("month", lit(last_month))

# Define GCS bucket path (Replace with your actual GCS bucket)
gcs_bucket_path = "gs://[YOUR-BUCKET-PATH]/credit_card_usage_report"

# Write to GCS in Parquet format with append mode
df.write \
    .mode("append") \
    .format("parquet") \
    .option("parquet.compression", "SNAPPY") \
    .partitionBy("year", "month") \
    .save(gcs_bucket_path)

print(f"Last month’s report ({last_year}-{last_month}) successfully saved to GCS in append mode!")
