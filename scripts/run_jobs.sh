#!/bin/bash

# This script automates the submission of write jobs to the Dataproc Serverless.

# Submit job for Parquet format with specific compression
gcloud dataproc jobs submit pyspark \
    --cluster=my-dataproc-cluster \
    --region=my-region \
    --py-files=process_data.py \
    --properties=spark.pyspark.python=/usr/bin/python3 \
    -- \
    process_data.py --input=bigquery_table --output=gs://my-bucket/parquet_output --format=parquet --compression=snappy

# Submit job for ORC format with specific compression
gcloud dataproc jobs submit pyspark \
    --cluster=my-dataproc-cluster \
    --region=my-region \
    --py-files=process_data.py \
    --properties=spark.pyspark.python=/usr/bin/python3 \
    -- \
    process_data.py --input=bigquery_table --output=gs://my-bucket/orc_output --format=orc --compression=zlib

# Submit job for Avro format with specific compression
gcloud dataproc jobs submit pyspark \
    --cluster=my-dataproc-cluster \
    --region=my-region \
    --py-files=process_data.py \
    --properties=spark.pyspark.python=/usr/bin/python3 \
    -- \
    process_data.py --input=bigquery_table --output=gs://my-bucket/avro_output --format=avro --compression=deflate

# Submit job for CSV format with specific compression
gcloud dataproc jobs submit pyspark \
    --cluster=my-dataproc-cluster \
    --region=my-region \
    --py-files=process_data.py \
    --properties=spark.pyspark.python=/usr/bin/python3 \
    -- \
    process_data.py --input=bigquery_table --output=gs://my-bucket/csv_output --format=csv --compression=gzip