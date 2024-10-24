import base64
import json
import os
import logging
from google.cloud import storage, dataproc_v1, monitoring_v3
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def collect_gcs_metrics(bucket_name, execution_id, data_format):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

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

    logger.info(f"Collected GCS metrics: {gcs_metrics}")
    return gcs_metrics

def collect_dataproc_metrics(project_id, region, batch_id):
    client = dataproc_v1.BatchControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    )

    name = f'projects/{project_id}/regions/{region}/batches/{batch_id}'
    try:
        batch = client.get_batch(name=name)
    except Exception as e:
        logger.error(f"Error getting batch {batch_id}: {e}")
        return {}

    state = batch.state.name
    create_time = batch.create_time
    start_time = batch.runtime_info.agent_start_time if batch.runtime_info else None
    end_time = batch.runtime_info.agent_end_time if batch.runtime_info else None

    dataproc_metrics = {
        "batch_id": batch_id,
        "state": state,
        "create_time": create_time.isoformat() if create_time else None,
        "start_time": start_time.isoformat() if start_time else None,
        "end_time": end_time.isoformat() if end_time else None,
    }

    logger.info(f"Collected basic Dataproc metrics for {batch_id}: {dataproc_metrics}")

    monitoring_client = monitoring_v3.MetricServiceClient(p

    interval = monitoring_v3.TimeInterval()
    end_time_monitoring = datetime.now(timezone.utc)

    start_time_monitoring = start_time if start_time else create_time
    if not start_time_monitoring:
        logger.error(f"Could not find start time for batch {batch_id}")
        return dataproc_metrics

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

    logger.info(f"Collected detailed Dataproc metrics for {batch_id}: {dataproc_metrics}")

    # Get executor metrics and task metrics
    executor_metrics = []
    task_metrics = []

    executor_query = f'metric.type="dataproc.googleapis.com/job/executor/total_vcore_seconds" AND resource.labels.job_id="{batch_id}"'
    task_query = f'metric.type="dataproc.googleapis.com/job/task/total_vcore_seconds" AND resource.labels.job_id="{batch_id}"'

    for query, metrics_list in [(executor_query, executor_metrics), (task_query, task_metrics)]:
        results = monitoring_client.list_time_series(
            request={
                "name": f"projects/{project_id}",
                "filter": query,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )

        for result in results:
            for point in result.points:
                metrics_list.append(point.value.double_value)
    
    dataproc_metrics["total_executor_vcore_seconds"] = sum(executor_metrics)
    dataproc_metrics["total_task_vcore_seconds"] = sum(task_metrics)

    return dataproc_metrics

def entry_point(request):
    # Parse request parameters
    request_json = request.get_json(silent=True)
    if not request_json:
        logger.error("No JSON payload received")
        return "Bad Request: no JSON payload", 400

    execution_id = request_json.get('execution_id')
    project_id = request_json.get('project_id')
    region = request_json.get('region')
    data_format = request_json.get('format', 'parquet')  # Default format if not provided
    bucket_name = request_json.get('bucket_name')  # Should be passed or set as an environment variable

    if not all([execution_id, project_id, region, bucket_name]):
        logger.error("Missing required parameters")
        return "Bad Request: missing required parameters", 400

    logger.info(f"Received parameters: execution_id={execution_id}, project_id={project_id}, region={region}")

    # Collect GCS metrics
    gcs_metrics = collect_gcs_metrics(bucket_name, execution_id, data_format)

    # Collect Dataproc metrics for both read and process batches
    dataproc_metrics = []
    for job_type in ['read-data', 'process-data']:
        batch_id = f"{job_type}-{execution_id}"
        metrics = collect_dataproc_metrics(project_id, region, batch_id)
        if metrics:
            dataproc_metrics.append(metrics)

    # Combine all metrics
    metrics = {
        "execution_id": execution_id,
        "format": data_format,
        "gcs_metrics": gcs_metrics,
        "dataproc_metrics": dataproc_metrics,
        "collection_time": datetime.now(timezone.utc).isoformat(),
    }

    # Save metrics to a local JSON file
    metrics_file = f"/tmp/collect_metrics_{execution_id}.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f)

    # Upload metrics file to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    metrics_blob = bucket.blob(f"metrics/collect_metrics_{execution_id}.json")
    metrics_blob.upload_from_filename(metrics_file)

    logger.info(f"Metrics uploaded to gs://{bucket_name}/metrics/collect_metrics_{execution_id}.json")

    return "Metrics collection completed", 200