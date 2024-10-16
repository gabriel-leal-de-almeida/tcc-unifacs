# entry_point.py for read_data

def entry_point(event, context):
    import base64
    import json
    import os
    from google.cloud import bigquery
    from google.cloud import pubsub_v1
    import uuid

    # Parse Pub/Sub message
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    project     = os.environ['PROJECT_ID']
    bucket      = os.environ['BUCKET_NAME']

    query = message.get('query')

    # BigQuery client
    client = bigquery.Client(project=project)

    random_id = str(uuid.uuid4())

    # Define destination table
    dataset_id = f"{project}.temp_dataset"
    table_id = f"{dataset_id}.temp_table_{random_id}"

    # Create dataset if it doesn't exist
    dataset_ref = bigquery.Dataset(dataset_id)
    dataset = client.create_dataset(dataset_ref, exists_ok=True)

    # Configure query job
    job_config = bigquery.QueryJobConfig(
        destination=table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Run the query
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for the job to complete

    # Extract table to GCS in Parquet format
    destination_uri = f"gs://{bucket}/source_data/"
    extract_job = client.extract_table(
        table_id,
        destination_uri,
        job_config=bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.PARQUET,
            compression=bigquery.Compression.SNAPPY
        )
    )
    extract_job.result()  # Wait for the job to complete

    # Delete temporary table
    client.delete_table(table_id, not_found_ok=True)