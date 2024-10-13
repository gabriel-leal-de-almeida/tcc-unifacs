import argparse
import logging
import json
import sys
import time
from google.cloud import storage
from google.cloud import dataproc_v1
from google.cloud import monitoring_v3
from datetime import datetime, timedelta
from google.protobuf.timestamp_pb2 import Timestamp

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Argumentos de linha de comando
parser = argparse.ArgumentParser()
parser.add_argument('--project', required=True, help='ID do projeto GCP')
parser.add_argument('--region', required=True, help='Região do Dataproc')
parser.add_argument('--bucket', required=True, help='Nome do bucket no GCS')
parser.add_argument('--execution_id', required=True, help='ID único da execução')
parser.add_argument('--format', required=True, help='Formato dos dados: parquet, avro, orc ou csv')
parser.add_argument('--batch_id', required=True, help='ID do batch job do Dataproc')
args = parser.parse_args()

def collect_gcs_metrics(bucket_name, execution_id, data_format):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    prefix = f"data/{data_format}/{execution_id}/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    total_size = sum(blob.size for blob in blobs)
    num_files = len(blobs)
    avg_file_size = total_size / num_files if num_files > 0 else 0

    # Coleta das datas de criação e modificação
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
    # Inicializa o cliente do Dataproc
    client = dataproc_v1.BatchControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    )

    name = f'projects/{project_id}/regions/{region}/batches/{batch_id}'
    batch = client.get_batch(name=name)

    # Obtém informações básicas do batch job
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

    # Coleta métricas de uso de recursos via Cloud Monitoring
    monitoring_client = monitoring_v3.MetricServiceClient()

    # Define o intervalo de tempo para as métricas
    interval = monitoring_v3.TimeInterval()
    end_time_monitoring = datetime.utcnow()
    start_time_monitoring = create_time

    interval.end_time.seconds = int(end_time_monitoring.timestamp())
    interval.start_time.seconds = int(create_time.timestamp())

    # Métrica de CPU (ajuste o filtro conforme necessário)
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

def main():
    # Coleta de métricas do GCS
    gcs_metrics = collect_gcs_metrics(args.bucket, args.execution_id, args.format)

    # Coleta de métricas do Dataproc Serverless
    dataproc_metrics = collect_dataproc_metrics(args.project, args.region, args.batch_id)

    # Consolidação das métricas
    metrics = {
        "execution_id": args.execution_id,
        "format": args.format,
        "gcs_metrics": gcs_metrics,
        "dataproc_metrics": dataproc_metrics,
        "collection_time": datetime.utcnow().isoformat(),
    }

    # Salvar as métricas em um arquivo JSON local
    metrics_file = f"/tmp/collect_metrics_{args.execution_id}.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f)

    logger.info(f"Métricas consolidadas salvas em {metrics_file}")

    # Copiar o arquivo de métricas para o GCS
    client = storage.Client()
    bucket = client.get_bucket(args.bucket)
    metrics_blob = bucket.blob(f"metrics/collect_metrics_{args.execution_id}.json")
    metrics_blob.upload_from_filename(metrics_file)

    logger.info(f"Métricas copiadas para gs://{args.bucket}/metrics/collect_metrics_{args.execution_id}.json")

if __name__ == "__main__":
    main()
