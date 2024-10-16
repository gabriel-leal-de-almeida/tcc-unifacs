# Create Cloud Functions


# Function for process_data.py
resource "google_cloudfunctions_function" "start_process_data_function" {
  name        = "start-process-data-function"
  description = "Cloud Function to execute process_data.py"
  runtime     = "python39"
  entry_point = "entry_point"
  available_memory_mb = 256
  timeout =  540
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.process_data_topic.id
  }

  source_archive_bucket = var.bucket_name
  source_archive_object = google_storage_bucket_object.start_process_data_functions_script.name

  environment_variables = {
    PROJECT_ID  = var.project_id
    REGION      = var.region
    BUCKET_NAME = var.bucket_name
    SCRIPT_PATH = "gs://${var.bucket_name}/${google_storage_bucket_object.process_data_dataproc_script.name}"
    NEXT_TOPIC  = google_pubsub_topic.read_data_topic.name
  }
}

# Function for read_data.py
resource "google_cloudfunctions_function" "start_read_data_function" {
  name        = "start-read-data-function"
  description = "Cloud Function to execute read_data.py"
  runtime     = "python39"
  entry_point = "entry_point"
  available_memory_mb = 256
  timeout = 540
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.read_data_topic.id
  }

  source_archive_bucket = var.bucket_name
  source_archive_object = google_storage_bucket_object.start_read_data_functions_script.name

  environment_variables = {
    PROJECT_ID  = var.project_id
    REGION      = var.region
    BUCKET_NAME = var.bucket_name
    SCRIPT_PATH = "gs://${var.bucket_name}/${google_storage_bucket_object.read_data_dataproc_script.name}"
    NEXT_TOPIC  = google_pubsub_topic.collect_metrics_topic.name
  }
}

# Function for collect_metrics.py
resource "google_cloudfunctions_function" "start_collect_metrics_function" {
  name        = "start-collect-metrics-function"
  description = "Cloud Function to execute collect_metrics.py"
  runtime     = "python39"
  entry_point = "entry_point"
  available_memory_mb = 256
  timeout = 540
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.collect_metrics_topic.id
  }

  source_archive_bucket = var.bucket_name
  source_archive_object = google_storage_bucket_object.start_collect_metrics_functions_script.name

  environment_variables = {
    PROJECT_ID  = var.project_id
    REGION      = var.region
    BUCKET_NAME = var.bucket_name
    SCRIPT_PATH = "gs://${var.bucket_name}/${google_storage_bucket_object.collect_metrics_functions_script.name}"
  }
}

resource "google_cloudfunctions_function" "write_bigquery_function" {
  name        = "start-write-bigquery-function"
  description = "Cloud Function to execute ..."
  runtime     = "python39"
  entry_point = "entry_point"
  available_memory_mb = 256
  timeout = 540
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.write_bigquery_topic.id
  }

  source_archive_bucket = var.bucket_name
  source_archive_object = google_storage_bucket_object.start_write_bigquery_functions_script.name

  environment_variables = {
    PROJECT_ID  = var.project_id
    BUCKET_NAME = var.bucket_name
    SCRIPT_PATH = "gs://${var.bucket_name}/${google_storage_bucket_object.start_write_bigquery_functions_script.name}"
  }
}