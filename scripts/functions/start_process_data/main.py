import base64
import json
import os
import uuid
from google.cloud import dataproc_v1

def entry_point(event, context):

    # Parse Pub/Sub message
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    project    = os.environ['PROJECT_ID']
    region     = os.environ['REGION']
    bucket     = os.environ['BUCKET_NAME']
    script_uri = os.environ['SCRIPT_PATH']

    execution_id = uuid.uuid4().hex
    data_format  = message.get('format')
    compression  = message.get('compression')

    # Set up Dataproc job
    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": script_uri,
            "jar_file_uris": [f"gs://{bucket}/libs/jars/spark-avro_2.13-3.5.1.jar"],
            "args": [
                "--project", project,
                "--bucket", bucket,
                "--format", data_format,
                "--compression", compression,
                "--execution_id", execution_id
            ]
        },
        "runtime_config": {
            "version": "2.2",
            "properties": {
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": f"gs://{bucket}/spark-event-logs/",
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.instances": "5",
                "spark.dataproc.driver.disk.size": "250g",
                "spark.driver.memory": "20g",
                "spark.driver.cores": "4",
                "spark.dataproc.executor.disk.size": "250g",
                "spark.executor.memory": "20g",
                "spark.executor.cores": "4"
            }
        },
        "labels": {
            "execution_id": execution_id
        }
    }

    parent = f"projects/{project}/locations/{region}"
    operation = client.create_batch(request={"parent": parent, "batch": batch, "batch_id": f"process-data-{data_format}-{execution_id}"})
    operation.result()
