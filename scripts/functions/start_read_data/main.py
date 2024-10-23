# entry_point.py for read_data

def entry_point(event, context):
    import base64
    import json
    import os
    from google.cloud import dataproc_v1
    from google.cloud import pubsub_v1

    # Parse Pub/Sub message
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    project    = os.environ['PROJECT_ID']
    region     = os.environ['REGION']
    bucket     = os.environ['BUCKET_NAME']
    script_uri = os.environ['SCRIPT_PATH']
    next_topic = os.environ['NEXT_TOPIC']

    execution_id = message.get('execution_id')
    data_format  = message.get('format')

    # Set up Dataproc job
    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": script_uri,
            "args": [
                "--project", project,
                "--bucket", bucket,
                "--format", data_format,
                "--execution_id", execution_id
            ]
        }
    }

    parent = f"projects/{project}/locations/{region}"
    operation = client.create_batch(request={"parent": parent, "batch": batch, "batch_id": f"read-data-{execution_id}"})
    operation.result()
