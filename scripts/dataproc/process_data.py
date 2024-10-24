# Importações necessárias
from pyspark.sql import SparkSession
import time
import uuid
import json
import logging
import argparse
import subprocess
import sys
from google.cloud import pubsub_v1

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parser de argumentos de linha de comando
parser = argparse.ArgumentParser()
parser.add_argument('--project', required=True, help='ID do projeto GCP')
parser.add_argument('--bucket', required=True, help='Nome do bucket no GCS para salvar os dados')
parser.add_argument('--format', required=True, help='Formato de saída: parquet, avro, orc ou csv')
parser.add_argument('--compression', required=True, help='Codec de compressão a ser utilizado')
parser.add_argument('--execution_id', required=True, help='ID único da execução')
args = parser.parse_args()
logger.info(f"Argumentos de linha de comando: {args}")

# Gera um ID único para a execução, caso não tenha sido fornecido
execution_id = args.execution_id

# Descrição da execução
description = f"Escrita em {args.format.upper()} com compressão {args.compression}"

# string_vazia = ""

# event_log_dir = f"gs://{args.bucket}/spark-event-logs/{execution_id}"
# logger.info(f"Logs do Spark serão salvos em {event_log_dir}")

# Inicializa a SparkSession com event logging habilitado
spark = SparkSession.builder \
    .appName(f"BigQuery to {args.format.upper()} - {execution_id}") \
    .getOrCreate()
    # .config("spark.eventLog.enabled", "true") \
    # .config("spark.eventLog.dir", f"{event_log_dir}") \

# Registro do tempo de início do job
job_start_time = time.time()
logger.info(f"Iniciando a execução com ID {execution_id}")

# Leitura dos dados do BigQuery
read_start_time = time.time()
logger.info("Avaliação do tempo de leitura a partir do BigQuery (origem)")

# Leitura do DataFrame
df = spark.read.format("bigquery") \
    .option("parentProject", f"{args.project}") \
    .option("bigQueryJobLabels", json.dumps({"execution_id": execution_id, "description": description, "format": args.format.lower()})) \
    .option("project", f"{args.project}") \
    .option("traceApplicationName", f"{args.project}.spark-job") \
    .option("traceJobId", f"{execution_id}") \
    .load(f"{args.project}.source_data.source_table")


read_end_time = time.time()
read_duration = read_end_time - read_start_time
logger.info(f"Tempo de avaliação de leitura a partir do BigQuery (origem): {read_duration} segundos")

# Escrita dos dados no formato especificado
write_start_time = time.time()
logger.info(f"Iniciando a escrita dos dados no formato {args.format.upper()} com compressão {args.compression}")

# Define o caminho completo de saída incluindo o ID de execução
output_full_path = f"gs://{args.bucket}/data/{args.format.lower()}/{execution_id}"

if args.format.lower() == 'csv':
    # Para CSV, a compressão é definida na opção 'codec'
    df.write \
        .option("header", "true") \
        .option("codec", args.compression) \
        .mode("overwrite") \
        .csv(output_full_path)
else:
    df.write \
        .option("compression", args.compression) \
        .mode("overwrite") \
        .format(args.format) \
        .save(output_full_path)

write_end_time = time.time()
write_duration = write_end_time - write_start_time
logger.info(f"Tempo de escrita no formato {args.format.upper()}: {write_duration} segundos")

# Registro do tempo total de execução
job_end_time = time.time()
total_duration = job_end_time - job_start_time
logger.info(f"Tempo total de execução: {total_duration} segundos")

metric_collector_start_time = time.time()
logger.info("Coleta de métricas")

# Coleta de métricas do tamanho dos dados escritos
# Usa o comando gsutil para obter o tamanho total dos arquivos
output_uri = f"{output_full_path}/*"
du_command = ['gsutil', 'du', '-s', output_uri] # 
du_process = subprocess.Popen(du_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
du_output, du_error = du_process.communicate()

if du_process.returncode == 0:
    size_in_bytes = int(du_output.decode('utf-8').split()[0])
    logger.info(f"Tamanho total dos dados escritos: {size_in_bytes} bytes")
else:
    size_in_bytes = None
    logger.error(f"Erro ao obter o tamanho dos dados escritos: {du_error.decode('utf-8')}")

# Coleta quantidade de arquivos escritos
# Usa o comando gsutil para contar a quantidade de arquivos
ls_command = ['gsutil', 'ls', output_uri]
ls_process = subprocess.Popen(ls_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
ls_output, ls_error = ls_process.communicate()

if ls_process.returncode == 0:
    num_files = len(ls_output.decode('utf-8').split())
    logger.info(f"Quantidade de arquivos escritos: {num_files}")    
    logger.info(f"Quantidade de arquivos de dados escritos (desconsidera 'path' da pasta do bucket e _SUCCESS): {num_files - 2}")
else:
    num_files = None
    logger.error(f"Erro ao obter a quantidade de arquivos escritos: {ls_error.decode('utf-8')}")

# Calcula o tamanho médio dos arquivos
avg_file_size = size_in_bytes / (num_files -2) if num_files else None # -2 para descontar os arquivos de metadados
logger.info(f"Tamanho médio dos arquivos escritos: {avg_file_size} bytes")

# Montagem das métricas
metrics = {
    "execution_id": execution_id,
    "description": description,
    "format": args.format.lower(),
    "compression": args.compression,
    "output_path": output_full_path,
    "read_duration_sec": read_duration,
    "write_duration_sec": write_duration,
    "total_duration_sec": total_duration,
    "size_in_bytes": size_in_bytes,
    "num_files": num_files,
    "avg_file_size_bytes": avg_file_size,
    "job_start_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job_start_time)),
    "job_end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job_end_time)),
    "spark_version": spark.version,
    "python_version": sys.version,
    # Outras métricas podem ser adicionadas aqui
}

# Salvar as métricas em um arquivo JSON local
metrics_file = f"/tmp/{execution_id}_metrics.json"
with open(metrics_file, "w") as f:
    json.dump(metrics, f)

logger.info(f"Métricas salvas em {metrics_file}")

# Copiar o arquivo de métricas para o GCS
gcs_metrics_path = f"gs://{args.bucket}/metrics/process-data/execution_id={execution_id}/metrics.json"
subprocess.run(['gsutil', 'cp', metrics_file, gcs_metrics_path])

logger.info(f"Métricas copiadas para {gcs_metrics_path}")

metric_collector_end_time = time.time()
metric_collector_duration = metric_collector_end_time - metric_collector_start_time
logger.info(f"Tempo de coleta de métricas: {metric_collector_duration} segundos")

# Encerra a SparkSession
spark.stop()
logger.info("SparkSession encerrada")

# Publish message to next topic
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(args.project, "read-data-topic")

next_message = {
    "execution_id": execution_id,
    "format": args.format
}

publisher.publish(topic_path, json.dumps(next_message).encode("utf-8"))

print(f"Mensagem publicada no tópico {topic_path}")
# Encerra a execução
print("Execução concluída com sucesso")