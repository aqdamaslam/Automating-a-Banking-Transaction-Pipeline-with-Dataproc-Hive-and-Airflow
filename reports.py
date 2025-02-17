from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, lit, upper
from datetime import datetime

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Monthly Credit Card Usage Report") \
    .enableHiveSupport() \
    .getOrCreate()

# Increase max fields displayed
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Define Hive database and table
hive_database = "[YOUR-HIVE-DATABASE]"
hive_table = "[YOUR-HIVE-TABLE]"

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
    print("No transactions found for the last month. Exiting...")
    exit()

# ------------------ Process Completed Transactions Only ------------------ #
df_completed = df.withColumn("transaction_status", upper(df.transaction_status)) \
                 .filter(df.transaction_status == "Completed")  

# Check if completed transactions exist before writing
if not df_completed.rdd.isEmpty():
    # Aggregate user-wise monthly spending for completed transactions
    usage_completed_df = df_completed.groupBy("card_number") \
        .agg(
            sum("transaction_amount").alias("total_spent"),
            count("transaction_id").alias("transaction_count"),
            avg("transaction_amount").alias("avg_transaction_amount")
        )

    # Join aggregated values with transaction details for completed transactions
    df_completed = df_completed.join(usage_completed_df, on="card_number", how="left")

    # Add year and month columns for partitioning
    df_completed = df_completed.withColumn("year", lit(last_year)).withColumn("month", lit(last_month))

    # Define GCS bucket path for completed transactions
    gcs_bucket_completed = "gs://[YOUR-GCP-BUCKET]/completed_transactions"

    # Write to GCS in Parquet format with append mode
    df_completed.write \
        .mode("append") \
        .format("parquet") \
        .option("parquet.compression", "SNAPPY") \
        .partitionBy("year", "month") \
        .save(gcs_bucket_completed)

    print(f" Completed transactions report ({last_year}-{last_month}) successfully saved to GCS!")
else:
    print(f" No completed transactions found for {last_year}-{last_month}. Skipping write to GCS.")

# ------------------ Process All Transactions (Including Failed & Pending) ------------------ #
# Group all transactions by `card_number` for the "All Transactions" report
df_all = df.withColumn("transaction_status", upper(df.transaction_status))

# Aggregate transaction details by `card_number` for all statuses
usage_all_df = df_all.groupBy("card_number") \
    .agg(
        sum("transaction_amount").alias("total_spent"),
        count("transaction_id").alias("transaction_count"),
        avg("transaction_amount").alias("avg_transaction_amount")
    )

# Join aggregated values with transaction details for all transactions
df_all = df_all.join(usage_all_df, on="card_number", how="left")

# Add year and month columns for partitioning
df_all = df_all.withColumn("year", lit(last_year)).withColumn("month", lit(last_month))

# Define GCS bucket path for all transactions
gcs_bucket_all = "gs://[YOUR-GCP-BUCKET]/all_transactions"

# Write all transactions to GCS in Parquet format with append mode
df_all.write \
    .mode("append") \
    .format("parquet") \
    .option("parquet.compression", "SNAPPY") \
    .partitionBy("year", "month") \
    .save(gcs_bucket_all)

print(f" All transactions report ({last_year}-{last_month}) successfully saved to GCS!")
