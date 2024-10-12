# This script reads the files stored in GCS and measures the read performance.

# TODO: Add your code here to read the files and measure the performance.

# Example code:
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read Data") \
    .getOrCreate()

# Read the files from GCS
df = spark.read.format("parquet").load("gs://your-bucket/path/to/parquet_files")

# Perform operations on the data
# ...

# Measure the read performance
# ...

# Print the results
# ...

# Stop the Spark session
spark.stop()