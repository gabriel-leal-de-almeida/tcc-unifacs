# process_data.py

# This script reads data from BigQuery and writes it to GCS in various formats (Parquet, ORC, Avro, CSV)
# with specific compressions.

# Import necessary libraries
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Process Data") \
    .getOrCreate()

# Read data from BigQuery
df = spark.read.format("bigquery") \
    .option("table", "your_bigquery_table") \
    .load()

# Write data to GCS in Parquet format with specific compression
df.write.format("parquet") \
    .option("compression", "snappy") \
    .save("gs://your_bucket/parquet_data")

# Write data to GCS in ORC format with specific compression
df.write.format("orc") \
    .option("compression", "zlib") \
    .save("gs://your_bucket/orc_data")

# Write data to GCS in Avro format with specific compression
df.write.format("avro") \
    .option("compression", "deflate") \
    .save("gs://your_bucket/avro_data")

# Write data to GCS in CSV format with specific compression
df.write.format("csv") \
    .option("compression", "gzip") \
    .save("gs://your_bucket/csv_data")

# Stop the Spark session
spark.stop()