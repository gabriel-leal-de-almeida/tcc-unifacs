# entry_point.py for read_data

def entry_point(event, context):
    import base64
    import json
    import os
    from google.cloud import bigquery

    # Parse Pub/Sub message
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    project = os.environ['PROJECT_ID']
    query   = message.get('query')

    # BigQuery client
    client = bigquery.Client(project=project)

    # Define destination table
    dataset_id = f"{project}.source_data"
    table_id   = f"{dataset_id}.source_table"

    # Create dataset if it doesn't exist
    dataset_ref = bigquery.Dataset(dataset_id)
    client.create_dataset(dataset_ref, exists_ok=True)

    # Configure query job
    job_config = bigquery.QueryJobConfig(
        destination=table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Run the query
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for the job to complete