# Importações necessárias
from pyspark.sql import SparkSession
import time
import uuid
import json
import logging
import argparse
import subprocess
import sys

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parser de argumentos de linha de comando
parser = argparse.ArgumentParser()
parser.add_argument('--project', required=True, help='ID do projeto GCP')
parser.add_argument('--bucket', required=True, help='Nome do bucket no GCS para salvar os dados')
parser.add_argument('--format', required=True, help='Formato de saída: parquet, avro, orc ou csv')
parser.add_argument('--compression', required=True, help='Codec de compressão a ser utilizado')
parser.add_argument('--execution_id', required=False, help='ID único da execução (opcional)')
args = parser.parse_args()

# Gera um ID único para a execução, caso não tenha sido fornecido
if args.execution_id:
    execution_id = args.execution_id
else:
    execution_id = str(uuid.uuid4())

# Descrição da execução
description = f"Escrita em {args.format.upper()} com compressão {args.compression}"

# Inicializa a SparkSession com event logging habilitado
spark = SparkSession.builder \
    .appName(f"BigQuery to {args.format.upper()} - {execution_id}") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", f"gs://{args.bucket}/spark-event-logs/{execution_id}") \
    .getOrCreate()

# Registro do tempo de início do job
job_start_time = time.time()
logger.info(f"Iniciando a execução com ID {execution_id}")

# Leitura dos dados do BigQuery
read_start_time = time.time()
logger.info("Avaliação do tempo de spark.read.format('bigquery').options(**bigquery_read_options).load()")

# Configuração adicional para capturar métricas do BigQuery
# Usando a opção 'parallelism' para controlar o número de partições
bigquery_read_options = {
    "query": """
        SELECT *
        FROM `bigquery-public-data.crypto_bitcoin.transactions`
        WHERE block_timestamp_month > '2024-01-01' AND block_timestamp_month < '2024-01-02'
    """,
    "parentProject": "{args.project}",
    "jobIdPrefix": f"job_{execution_id}_"
}


# Leitura do DataFrame
df = spark.read.format("bigquery") \
    .options(**bigquery_read_options) \
    .load()

read_end_time = time.time()
read_duration = read_end_time - read_start_time
logger.info(f"Tempo de avaliação de spark.read.format('bigquery').options(**bigquery_read_options).load(): {read_duration} segundos")

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
output_uri = f"{output_full_path}/"
du_command = ['gsutil', 'du', '-s', output_uri]
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
else:
    num_files = None
    logger.error(f"Erro ao obter a quantidade de arquivos escritos: {ls_error.decode('utf-8')}")

# Calcula o tamanho médio dos arquivos
avg_file_size = size_in_bytes / num_files if num_files else None
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
gcs_metrics_path = f"gs://{args.bucket}/metrics/{execution_id}_metrics.json"
subprocess.run(['gsutil', 'cp', metrics_file, gcs_metrics_path])

logger.info(f"Métricas copiadas para {gcs_metrics_path}")

metric_collector_end_time = time.time()
metric_collector_duration = metric_collector_end_time - metric_collector_start_time
logger.info(f"Tempo de coleta de métricas: {metric_collector_duration} segundos")

# Encerra a SparkSession
spark.stop()
logger.info("SparkSession encerrada")