# read_data.py

import argparse
import logging
import time
import json
import sys
from pyspark.sql import SparkSession
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--project', required=True, help='ID do projeto GCP')
parser.add_argument('--bucket', required=True, help='Nome do bucket no GCS para salvar as métricas')
parser.add_argument('--format', required=True, help='Formato dos dados: parquet, avro, orc ou csv')
parser.add_argument('--execution_id', required=True, help='ID único da execução')
args = parser.parse_args()
logger.info(f"Argumentos de linha de comando: {args}")

# Obtém os argumentos de linha de comando
execution_id = args.execution_id

# Descrição
description = f"Leitura de dados no formato {args.format.upper()}"

# Constrói o caminho do GCS para os dados que serão lidos da etapa anterior
input_path = f"gs://{args.bucket}/data/{args.format}/{execution_id}/"


# event_log_dir = f"gs://{args.bucket}/spark-event-logs/{execution_id}"
# logger.info(f"Logs do Spark serão salvos em {event_log_dir}")

# Inicializa a SparkSession
spark = SparkSession.builder \
    .appName(f"Read {args.format.upper()} Data - {execution_id}") \
    .getOrCreate()
    # .config("spark.eventLog.enabled", "true") \
    # .config("spark.eventLog.dir", f"{event_log_dir}") \

# Registro do tempo de início do job
job_start_time = time.time()
logger.info(f"Iniciando a execução com ID {execution_id}")

# Leitura dos dados e medição do tempo
read_start_time = time.time()
logger.info(f"Iniciando a leitura dos dados de {input_path} no formato {args.format.upper()}")

if args.format.lower() == 'csv':
    df = spark.read \
        .option("header", "true") \
        .csv(input_path)
else:
    df = spark.read.format(args.format.lower()).load(input_path)

read_end_time = time.time()
read_duration = read_end_time - read_start_time
logger.info(f"Leitura de dados concluída em {read_duration} segundos")

# Realiza uma ação para garantir que os dados foram lidos
count_start_time = time.time()
record_count = df.count()
count_end_time = time.time()
count_duration = count_end_time - count_start_time
logger.info(f"Contagem de registros concluída em {count_duration} segundos")
logger.info(f"Total de registros lidos: {record_count}")

# Registro do tempo total de execução
job_end_time = time.time()
total_duration = job_end_time - job_start_time
logger.info(f"Tempo total de execução: {total_duration} segundos")

# Coleta das métricas
metrics = {
    "execution_id": execution_id,
    "description": description,
    "format": args.format.lower(),
    "input_path": input_path,
    "read_duration_sec": read_duration,
    "count_duration_sec": count_duration,
    "total_duration_sec": total_duration,
    "record_count": record_count,
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

# Encerra a SparkSession
spark.stop()
logger.info("SparkSession encerrada")