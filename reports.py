from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, year, month

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Monthly Usage Report") \
    .enableHiveSupport() \
    .getOrCreate()

# Define Hive database and table
hive_database = "[YOUR--HIVE-DATABASE]"
hive_table = "[YOUR--HIVE-DATABASE-TABLE]"

# Read data from Hive table
df = spark.sql(f"SELECT * FROM {hive_database}.{hive_table}")

# Extract year and month from transaction_date
df = df.withColumn("year", year(df["transaction_date"])) \
       .withColumn("month", month(df["transaction_date"]))

# Aggregate monthly usage per card holder
monthly_usage_df = df.groupBy("card_holder_name", "year", "month") \
    .agg(
        sum("transaction_amount").alias("total_spent"),
        count("transaction_id").alias("transaction_count"),
        avg("transaction_amount").alias("avg_transaction_amount")
    )

# Define GCS bucket path (Replace with your actual GCS bucket name)
gcs_bucket_path = "gs://your-gcs-bucket-name/monthly_usage_report"

# Write the report to GCS in Parquet format, partitioned by year and month
monthly_usage_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("parquet.compression", "SNAPPY") \
    .partitionBy("year", "month") \
    .save(gcs_bucket_path)

print("Monthly usage report successfully saved to GCS with year/month partitioning!")
