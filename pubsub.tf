# Create Pub/Sub topics
resource "google_pubsub_topic" "process_data_topic" {
  name = "process-data-topic"
}

resource "google_pubsub_topic" "read_data_topic" {
  name = "read-data-topic"
}

resource "google_pubsub_topic" "collect_metrics_topic" {
  name = "collect-metrics-topic"
}