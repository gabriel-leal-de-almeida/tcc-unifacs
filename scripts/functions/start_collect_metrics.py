import base64
import json
import os
import logging
from google.cloud import storage, dataproc_v1, monitoring_v3
from datetime import datetime

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def collect_gcs_metrics(bucket_name, execution_id, data_format):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    prefix = f"data/{data_format}/{execution_id}/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    total_size = sum(blob.size for blob in blobs)
    num_files = len(blobs)
    avg_file_size = total_size / num_files if num_files > 0 else 0

    creation_times = [blob.time_created for blob in blobs]
    update_times = [blob.updated for blob in blobs]
    earliest_creation = min(creation_times) if creation_times else None
    latest_update = max(update_times) if update_times else None

    gcs_metrics = {
        "total_size_bytes": total_size,
        "num_files": num_files,
        "avg_file_size_bytes": avg_file_size,
        "earliest_creation_time": earliest_creation.isoformat() if earliest_creation else None,
        "latest_update_time": latest_update.isoformat() if latest_update else None,
    }

    logger.info(f"Métricas do GCS coletadas: {gcs_metrics}")
    return gcs_metrics

def collect_dataproc_metrics(project_id, region, batch_id):
    client = dataproc_v1.BatchControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    )

    name = f'projects/{project_id}/regions/{region}/batches/{batch_id}'
    batch = client.get_batch(name=name)

    state = batch.state.name
    create_time = batch.create_time
    start_time = batch.runtime_info.agent_start_time if batch.runtime_info else None
    end_time = batch.runtime_info.agent_end_time if batch.runtime_info else None

    dataproc_metrics = {
        "state": state,
        "create_time": create_time.isoformat() if create_time else None,
        "start_time": start_time.isoformat() if start_time else None,
        "end_time": end_time.isoformat() if end_time else None,
    }

    logger.info(f"Métricas básicas do Dataproc coletadas: {dataproc_metrics}")

    monitoring_client = monitoring_v3.MetricServiceClient()

    interval = monitoring_v3.TimeInterval()
    end_time_monitoring = datetime.utcnow()
    start_time_monitoring = create_time

    interval.end_time.seconds = int(end_time_monitoring.timestamp())
    interval.start_time.seconds = int(create_time.timestamp())

    cpu_query = f'metric.type="dataproc.googleapis.com/job/yarn_cpu_usage" AND resource.labels.job_id="{batch_id}"'

    results = monitoring_client.list_time_series(
        request={
            "name": f"projects/{project_id}",
            "filter": cpu_query,
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )

    cpu_usage = []
    for result in results:
        for point in result.points:
            cpu_usage.append(point.value.double_value)

    avg_cpu_usage = sum(cpu_usage) / len(cpu_usage) if cpu_usage else 0

    dataproc_metrics["avg_cpu_usage_cores"] = avg_cpu_usage

    logger.info(f"Métricas detalhadas do Dataproc coletadas: {dataproc_metrics}")
    return dataproc_metrics

def entry_point(event, context):
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    project    = os.environ['PROJECT_ID']
    region     = os.environ['REGION']
    bucket     = os.environ['BUCKET_NAME']
    script_uri = os.environ['SCRIPT_PATH']

    execution_id = message.get('execution_id')
    data_format  = message.get('format')

    batch_id = f"read-data-{execution_id}"

    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": script_uri,
            "args": [
                "--project", project,
                "--region", region,
                "--bucket", bucket,
                "--format", data_format,
                "--execution_id", execution_id,
                "--batch_id", batch_id
            ]
        }
    }

    parent = f"projects/{project}/locations/{region}"
    operation = client.create_batch(request={"parent": parent, "batch": batch, "batch_id": f"collect-metrics-{execution_id}"})
    operation.result()

    gcs_metrics = collect_gcs_metrics(bucket, execution_id, data_format)
    dataproc_metrics = collect_dataproc_metrics(project, region, batch_id)

    metrics = {
        "execution_id": execution_id,
        "format": data_format,
        "gcs_metrics": gcs_metrics,
        "dataproc_metrics": dataproc_metrics,
        "collection_time": datetime.utcnow().isoformat(),
    }

    metrics_file = f"/tmp/collect_metrics_{execution_id}.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f)

    client = storage.Client()
    bucket = client.get_bucket(bucket)
    metrics_blob = bucket.blob(f"metrics/collect_metrics_{execution_id}.json")
    metrics_blob.upload_from_filename(metrics_file)

    logger.info(f"Métricas copiadas para gs://{bucket}/metrics/collect_metrics_{execution_id}.json")

if __name__ == "__main__":
    entry_point()