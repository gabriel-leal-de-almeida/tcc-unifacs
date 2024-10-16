# Create Cloud Scheduler jobs for each format

locals {
  formats = {
    parquet = {
      execution_id = "${uuid()}"
      format       = "parquet"
      compression  = "snappy"
    }
    avro = {
      execution_id = "${uuid()}"
      format       = "avro"
      compression  = "snappy"
    }
    orc = {
      execution_id = "${uuid()}"
      format       = "orc"
      compression  = "snappy"
    }
    csv = {
      execution_id = "${uuid()}"
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
      "query" : "SELECT * FROM `bigquery-public-data.crypto_bitcoin.transactions` WHERE block_timestamp_month = '2024-10-16'"
    }))
  }
}