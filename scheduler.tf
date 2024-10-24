# Create Cloud Scheduler jobs for each format

locals {
  formats = {
    parquet = {
      format       = "parquet"
      compression  = "snappy"
    }
    avro = {
      format       = "avro"
      compression  = "snappy"
    }
    orc = {
      format       = "orc"
      compression  = "snappy"
    }
    csv = {
      format       = "csv"
      compression  = "gzip"
    }
  }
}

resource "google_cloud_scheduler_job" "start_pipeline" {
  for_each    = local.formats
  name        = "start-pipeline-job-${each.key}"
  description = "Job to start the data processing pipeline for ${each.key}"
  schedule    = "1 0 1 1 *"
  time_zone   = "America/Sao_Paulo"

  pubsub_target {
    topic_name = google_pubsub_topic.process_data_topic.id
    data       = base64encode(jsonencode(each.value))
  }
}

resource "google_cloud_scheduler_job" "start_write_bigquery" {
  name        = "start-write-bigquery"
  description = "Job to write BigQuery results"
  schedule    = "1 0 1 1 *"
  time_zone   = "America/Sao_Paulo"

  pubsub_target {
    topic_name = google_pubsub_topic.write_bigquery_topic.id
    data       = base64encode(jsonencode({
      "query" : "SELECT * FROM `bigquery-public-data.crypto_bitcoin.transactions` WHERE block_timestamp_month > DATE('2022-12-31') AND block_timestamp_month < DATE('2024-01-01')"
    }))
  }
}

resource "google_cloud_scheduler_job" "start_write_bigquery_dev" {
  name        = "start-write-bigquery-dev"
  description = "DEV: Job to write BigQuery results"
  schedule    = "1 0 1 1 *"
  time_zone   = "America/Sao_Paulo"

  pubsub_target {
    topic_name = google_pubsub_topic.write_bigquery_topic.id
    data       = base64encode(jsonencode({
      "query" : "SELECT `hash`, `size`, `virtual_size`, `version`, `lock_time`, `block_hash`, `block_number`, `block_timestamp`, `block_timestamp_month`, `input_count`, `output_count`, `input_value`, `output_value`, `is_coinbase`, `fee` FROM `bigquery-public-data.crypto_bitcoin.transactions` WHERE block_timestamp_month > DATE('2022-12-31') AND block_timestamp_month < DATE('2023-02-01') LIMIT 100000"
    }))
  }
}