resource "google_bigquery_dataset" "metrics" {
    dataset_id = "metrics"
    project = var.project_id
    location = var.region
    default_table_expiration_ms = 3600000
    default_partition_expiration_ms = 3600000
    labels = {
        terraform = "true"
    }
}

resource "google_bigquery_table" "process_data_metrics" {
    dataset_id = google_bigquery_dataset.metrics.dataset_id
    table_id = "process_data_metrics"
    project = var.project_id
    deletion_protection = true

    schema = jsonencode([
        {
            name = "spark_version"
            type = "STRING"
            mode = "REQUIRED"
            description = "A versão do Spark utilizada para processar os dados."
        },
        {
            name = "python_version"
            type = "STRING"
            mode = "REQUIRED"
            description = "A versão do Python utilizada para processar os dados."
        },
        {
            name = "execution_id"
            type = "STRING"
            mode = "REQUIRED"
            description = "O ID da execução do job."
        },
        {
            name = "description"
            type = "STRING"
            mode = "NULLABLE"
            description = "A descrição do job."
        },
        {
            name = "format"
            type = "STRING"
            mode = "REQUIRED"
            description = "O formato de escrita dos dados."
        },
        {
            name = "compression"
            type = "STRING"
            mode = "NULLABLE"
            description = "O tipo de compressão utilizado para escrever os dados."
        },
        {
            name = "output_path"
            type = "STRING"
            mode = "REQUIRED"
            description = "O caminho de escrita dos dados."
        },
        {
            name = "read_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de leitura dos dados a partir do BigQuery."
        },
        {
            name = "write_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de escrita dos dados no GCS."
        },
        {
            name = "job_start_time"
            type = "TIMESTAMP"
            mode = "REQUIRED"
            description = "O horário de início do job."
        },
        {
            name = "job_end_time"
            type = "TIMESTAMP"
            mode = "REQUIRED"
            description = "O horário de término do job."
        },
        {
            name = "total_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo total de execução do job."
        },
        {
            name = "size_in_bytes"
            type = "INTEGER"
            mode = "NULLABLE"
            description = "O tamanho total dos dados escritos no GCS."
        },
        {
            name = "num_files"
            type = "INTEGER"
            mode = "NULLABLE"
            description = "O número de arquivos escritos no GCS (desprezando path de diretório e arquivo _SUCCESS)."
        },
        {
            name = "avg_file_size_bytes"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tamanho médio dos arquivos escritos no GCS."
        },
        {
            name = "metric_colletor_start_time"
            type = "TIMESTAMP"
            mode = "REQUIRED"
            description = "O horário de início da coleta de métricas."
        },
        {
            name = "metric_colletor_end_time"
            type = "TIMESTAMP"
            mode = "REQUIRED"
            description = "O horário de término da coleta de métricas."
        },
        {
            name = "metric_colletor_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo total de coleta de métricas."
        }
    ])

    external_data_configuration {
        autodetect = false
        source_format = "NEWLINE_DELIMITED_JSON"
        source_uris = ["gs://${var.bucket_name}/metrics/process-data/*.json"]
    }
}

resource "google_bigquery_table" "read_data_metrics" {
    dataset_id = google_bigquery_dataset.metrics.dataset_id
    table_id = "read_data_metrics"
    project = var.project_id
    deletion_protection = true

    schema = jsonencode([
        {
            name = "spark_version"
            type = "STRING"
            mode = "REQUIRED"
            description = "A versão do Spark utilizada para processar os dados."
        },
        {
            name = "python_version"
            type = "STRING"
            mode = "REQUIRED"
            description = "A versão do Python utilizada para processar os dados."
        },
        {
            name = "execution_id"
            type = "STRING"
            mode = "REQUIRED"
            description = "O ID da execução do job."
        },
        {
            name = "description"
            type = "STRING"
            mode = "NULLABLE"
            description = "A descrição do job."
        },
        {
            name = "format"
            type = "STRING"
            mode = "REQUIRED"
            description = "O formato de leitura dos dados."
        },
        {
            name = "input_path"
            type = "STRING"
            mode = "REQUIRED"
            description = "O caminho de leitura dos dados."
        },
        {
            name = "read_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de leitura dos dados em segundos."
        },
        {
            name = "count_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de contagem dos registros em segundos."
        },
        {
            name = "record_count"
            type = "INTEGER"
            mode = "NULLABLE"
            description = "O número total de registros lidos."
        },
        {
            name = "distinct_count"
            type = "INTEGER"
            mode = "NULLABLE"
            description = "O número total de registros distintos."
        },
        {
            name = "aggregation_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de agregação dos dados em segundos."
        },
        {
            name = "filtered_count"
            type = "INTEGER"
            mode = "NULLABLE"
            description = "O número de registros filtrados."
        },
        {
            name = "filter_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de filtragem dos dados em segundos."
        },
        {
            name = "sort_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de ordenação dos dados em segundos."
        },
        {
            name = "select_columns_concat_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de seleção das colunas com concatenção de dados e contagem das linhas distintas em segundos."
        },
        {
            name = "time_range_filter_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de filtragem por intervalo de tempo em segundos."
        },
        {
            name = "numeric_filter_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo de filtragem numérica em segundos."
        },
        {
            name = "job_start_time"
            type = "TIMESTAMP"
            mode = "REQUIRED"
            description = "O horário de início do job."
        },
        {
            name = "job_end_time"
            type = "TIMESTAMP"
            mode = "REQUIRED"
            description = "O horário de término do job em segundos."
        },
        {
            name = "total_duration_sec"
            type = "FLOAT"
            mode = "NULLABLE"
            description = "O tempo total de execução do job em segundos."
        }
    ])

    external_data_configuration {
        autodetect = false
        source_format = "NEWLINE_DELIMITED_JSON"
        source_uris = ["gs://${var.bucket_name}/metrics/read-data/*.json"]
    }
}

# Create complete_metrics view that JOINs process_data_metrics and read_data_metrics on execution_id and adds the prefix 'process_data_' to the columns of process_data_metrics and the prefix 'read_data_' to the columns of read_data_metrics.
resource "google_bigquery_table" "complete_metrics" {
    dataset_id = google_bigquery_dataset.metrics.dataset_id
    table_id = "complete_metrics"
    project = var.project_id
    deletion_protection = true

    view {
        query = <<-SQL
            SELECT
                process_data_metrics.spark_version AS process_data_spark_version,
                process_data_metrics.python_version AS process_data_python_version,
                process_data_metrics.execution_id AS process_data_execution_id,
                process_data_metrics.description AS process_data_description,
                process_data_metrics.format AS process_data_format,
                process_data_metrics.compression AS process_data_compression,
                process_data_metrics.output_path AS process_data_output_path,
                process_data_metrics.read_duration_sec AS process_data_read_duration_sec,
                process_data_metrics.write_duration_sec AS process_data_write_duration_sec,
                process_data_metrics.job_start_time AS process_data_job_start_time,
                process_data_metrics.job_end_time AS process_data_job_end_time,
                process_data_metrics.total_duration_sec AS process_data_total_duration_sec,
                process_data_metrics.size_in_bytes AS process_data_size_in_bytes,
                process_data_metrics.num_files AS process_data_num_files,
                process_data_metrics.avg_file_size_bytes AS process_data_avg_file_size_bytes,
                process_data_metrics.metric_colletor_start_time AS process_data_metric_colletor_start_time,
                process_data_metrics.metric_colletor_end_time AS process_data_metric_colletor_end_time,
                process_data_metrics.metric_colletor_duration_sec AS process_data_metric_colletor_duration_sec,
                read_data_metrics.spark_version AS read_data_spark_version,
                read_data_metrics.python_version AS read_data_python_version,
                read_data_metrics.execution_id AS read_data_execution_id,
                read_data_metrics.description AS read_data_description,
                read_data_metrics.format AS read_data_format,
                read_data_metrics.input_path AS read_data_input_path,
                read_data_metrics.read_duration_sec AS read_data_read_duration_sec,
                read_data_metrics.count_duration_sec AS read_data_count_duration_sec,
                read_data_metrics.record_count AS read_data_record_count,
                read_data_metrics.distinct_count AS read_data_distinct_count,
                read_data_metrics.aggregation_duration_sec AS read_data_aggregation_duration_sec,
                read_data_metrics.filtered_count AS read_data_filtered_count,
                read_data_metrics.filter_duration_sec AS read_data_filter_duration_sec,
                read_data_metrics.sort_duration_sec AS read_data_sort_duration_sec,
                read_data_metrics.select_columns_concat_duration_sec AS read_data_select_columns_concat_duration_sec,
                read_data_metrics.time_range_filter_duration_sec AS read_data_time_range_filter_duration_sec,
                read_data_metrics.numeric_filter_duration_sec AS read_data_numeric_filter_duration_sec,
                read_data_metrics.job_start_time AS read_data_job_start_time,
                read_data_metrics.job_end_time AS read_data_job_end_time,
                read_data_metrics.total_duration_sec AS read_data_total_duration_sec
            FROM
                "${var.project_id}.${google_bigquery_dataset.metrics.dataset_id}.process_data_metrics" AS process_data_metrics
            JOIN
                "${var.project_id}.${google_bigquery_dataset.metrics.dataset_id}.read_data_metrics" AS read_data_metrics ON
                    process_data_metrics.execution_id = read_data_metrics.execution_id
        SQL
    }
}