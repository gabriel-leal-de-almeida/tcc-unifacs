# entry_point.py for read_data

def entry_point(event, context):
    import base64
    import json
    import os
    from google.cloud import bigquery
    import logging

    logging.basicConfig(level=logging.INFO)

    try:
        # Parse Pub/Sub message
        data = base64.b64decode(event['data']).decode('utf-8')
        message = json.loads(data)
        logging.info(f"Received message: {message}")

        project = os.environ['PROJECT_ID']
        query = message.get('query')
        logging.info(f"Query: {query}")

        if not query:
            logging.error("No query found in the message.")
            return

        # BigQuery client
        client = bigquery.Client(project=project)

        # Define destination table
        dataset_id = f"{project}.source_data"
        table_id = f"{dataset_id}.source_table"

        # Create dataset if it doesn't exist
        dataset_ref = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset_ref, exists_ok=True)

        # Configure query job
        job_config = bigquery.QueryJobConfig(
            destination=table_id,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Run the query
        logging.info("Running query job...")
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        logging.info("Query job completed.")

        # Check the number of rows in the destination table
        table = client.get_table(table_id)
        row_count = table.num_rows
        logging.info(f"Number of rows in the table: {row_count}")

        if row_count == 0:
            logging.warning("The query executed successfully, but no data was inserted into the table.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")