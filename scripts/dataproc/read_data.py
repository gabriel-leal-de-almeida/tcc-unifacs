# read_data.py

import argparse
import logging
import time
import json
import sys
from pyspark.sql import SparkSession
import subprocess
from google.cloud import pubsub_v1
from pyspark.sql import functions as F

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

# Inicializa a SparkSession
spark = SparkSession.builder \
    .appName(f"read-data-{args.format}-{execution_id}") \
    .getOrCreate()

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

# Realiza uma ação de contagem
count_start_time = time.time()
record_count = df.count()
count_end_time = time.time()
count_duration = count_end_time - count_start_time
logger.info(f"Total de registros lidos: {record_count}")
logger.info(f"Contagem de registros concluída em {count_duration} segundos")

# Realiza a contagem de registros distintos
distinct_count_start_time = time.time()
distinct_count = df.distinct().count()
distinct_count_end_time = time.time()
distinct_count_duration = distinct_count_end_time - distinct_count_start_time
logger.info(f"Total de registros distintos: {distinct_count}")
logger.info(f"Contagem de registros distintos concluída em {distinct_count_duration} segundos")

# Realiza uma agregação
aggregation_start_time = time.time()
aggregated_df = df.groupBy("block_timestamp_month", "is_coinbase") \
    .agg(
        F.min("block_timestamp").alias("timestamp_minimo"),
        F.max("block_timestamp").alias("timestamp_maximo"),
        F.count("*").alias("quantidade_de_linhas"),
        F.sum("fee").alias("soma_fee")
    )
aggregated_df_count = aggregated_df.count()
aggregation_end_time = time.time()
aggregation_duration = aggregation_end_time - aggregation_start_time
logger.info(f"Total de registros após agregação: {aggregated_df_count}")
logger.info(f"Agregação concluída em {aggregation_duration} segundos")

# Realiza uma filtragem
filter_start_time = time.time()
filtered_df = df.filter(df['is_coinbase'] == True)
filtered_count = filtered_df.count()
filter_end_time = time.time()
filter_duration = filter_end_time - filter_start_time
logger.info(f"Filtragem concluída em {filter_duration} segundos")
logger.info(f"Total de registros após filtragem: {filtered_count}")

# Realiza uma ordenação
sort_start_time = time.time()
sorted_df = df.orderBy(F.desc("block_timestamp"))
sorted_df.limit(int(0.01*record_count)).collect()
sort_end_time = time.time()
sort_duration = sort_end_time - sort_start_time
logger.info(f"Ordenação concluída em {sort_duration} segundos")

# Realiza a seleção de colunas específicas
select_columns_start_time = time.time()
select_columns_df = df.select("block_timestamp", "fee", "is_coinbase")
select_columns_df.limit(int(0.01*record_count)).collect()
select_columns_end_time = time.time()
select_columns_read_duration = select_columns_end_time - select_columns_start_time
logger.info(f"Leitura de colunas específicas concluída em {select_columns_read_duration} segundos")

# Realiza a filtragem por intervalo de tempo
time_range_filter_start_time = time.time()
filtered_by_date_df = df.filter((df.block_timestamp_month >= "2023-01-01") & (df.block_timestamp_month < "2023-03-01"))
filtered_by_date_df_count = filtered_by_date_df.count()
time_range_filter_end_time = time.time()
time_range_filter_duration = time_range_filter_end_time - time_range_filter_start_time
logger.info(f"Quantidade de registros após filtragem por intervalo de tempo: {filtered_by_date_df_count}")
logger.info(f"Filtragem por intervalo de tempo concluída em {time_range_filter_duration} segundos")

# Leitura com filtro numérico (filtra apenas valores pares)
numeric_filter_start_time = time.time()
numeric_filtered_df = df.filter(df.block_number % 2 == 0)
numeric_filtered_df_count = numeric_filtered_df.count()
numeric_filter_end_time = time.time()
numeric_filter_duration = numeric_filter_end_time - numeric_filter_start_time
logger.info(f"Quantidade de registros após filtragem numérica: {numeric_filtered_df_count}")
logger.info(f"Filtragem numérica concluída em {numeric_filter_duration} segundos")

# Registro do tempo total de execução
job_end_time = time.time()
total_duration = job_end_time - job_start_time
logger.info(f"Tempo total de execução: {total_duration} segundos")

# Coleta das métricas
metrics = {
    "spark_version": spark.version,
    "python_version": sys.version,
    "execution_id": execution_id,
    "description": description,
    "format": args.format.lower(),
    "input_path": input_path,
    "read_duration_sec": read_duration,
    "count_duration_sec": count_duration,
    "record_count": record_count,
    "distinct_count": distinct_count,
    "aggregation_duration_sec": aggregation_duration,
    "filtered_count": filtered_count,
    "filter_duration_sec": filter_duration,
    "sort_duration_sec": sort_duration,
    "select_columns_duration_sec": select_columns_read_duration,
    "time_range_filter_duration_sec": time_range_filter_duration,
    "numeric_filter_duration_sec": numeric_filter_duration,
    "job_start_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job_start_time)),
    "job_end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job_end_time)),
    "total_duration_sec": total_duration,
    # Outras métricas podem ser adicionadas aqui
}

# Salvar as métricas em um arquivo JSON local
metrics_file = f"/tmp/{execution_id}_metrics.json"
with open(metrics_file, "w") as f:
    json.dump(metrics, f)

logger.info(f"Métricas salvas em {metrics_file}")

# Copiar o arquivo de métricas para o GCS
gcs_metrics_path = f"gs://{args.bucket}/metrics/read-data/execution_id={execution_id}/metrics.json"
subprocess.run(['gsutil', 'cp', metrics_file, gcs_metrics_path])

logger.info(f"Métricas copiadas para {gcs_metrics_path}")

# Limpando dados do bucket para evitar custos adicionais de armaazenamento
subprocess.run(['gsutil', '-m', 'rm', '-r', input_path])

logger.info(f"Dados removidos de {input_path}")

spark.stop()
logger.info("SparkSession encerrada")