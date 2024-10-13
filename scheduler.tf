# Create Cloud Scheduler job to start the pipeline
resource "google_cloud_scheduler_job" "start_pipeline" {
  name        = "start-pipeline-job"
  description = "Job to start the data processing pipeline"
  schedule    = "1 0 1 1 *"
  time_zone   = "America/Sao_Paulo"

  pubsub_target {
    topic_name = google_pubsub_topic.process_data_topic.id
    data       = base64encode(jsonencode({
      "execution_id" : "${uuid()}",    # Generates a new UUID for execution_id
      "format"       : "parquet",
      "compression"  : "snappy"
    }))
  }
}