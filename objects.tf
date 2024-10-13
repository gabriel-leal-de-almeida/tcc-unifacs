# Carrega os scripts para o bucket do Google Cloud Storage
resource "google_storage_bucket_object" "process_data_dataproc_script" {
  name   = "${var.dataproc_scripts_folder}process_data.py"
  bucket = var.bucket_name
  source = "${path.module}/${var.dataproc_scripts_folder}/process_data.py"
}

resource "google_storage_bucket_object" "read_data_dataproc_script" {
  name   = "${var.dataproc_scripts_folder}read_data.py"
  bucket = var.bucket_name
  source = "${path.module}/${var.dataproc_scripts_folder}/read_data.py"
}

resource "google_storage_bucket_object" "collect_metrics_functions_script" {
  name   = "${var.functions_scripts_folder}collect_metrics.py"
  bucket = var.bucket_name
  source = "${path.module}/${var.functions_scripts_folder}/collect_metrics.py"
}

# Upload scripts to GCS
# Zip the scripts using local-exec provisioner
resource "null_resource" "zip_scripts" {
  provisioner "local-exec" {
    command = <<EOT
      mkdir -p ${path.module}/zips
      zip -j ${path.module}/zips/start_process_data.zip ${path.module}/${var.functions_scripts_folder}/start_process_data.py
      zip -j ${path.module}/zips/start_read_data.zip ${path.module}/${var.functions_scripts_folder}/start_read_data.py
      zip -j ${path.module}/zips/start_collect_metrics.zip ${path.module}/${var.functions_scripts_folder}/start_collect_metrics.py
    EOT
  }
}

# Upload zipped scripts to GCS
resource "google_storage_bucket_object" "start_process_data_functions_script" {
  name   = "${var.functions_scripts_folder}start_process_data.zip"
  bucket = var.bucket_name
  source = "${path.module}/zips/start_process_data.zip"
  depends_on = [null_resource.zip_scripts]
}

resource "google_storage_bucket_object" "start_read_data_functions_script" {
  name   = "${var.functions_scripts_folder}start_read_data.zip"
  bucket = var.bucket_name
  source = "${path.module}/zips/start_read_data.zip"
  depends_on = [null_resource.zip_scripts]
}

resource "google_storage_bucket_object" "start_collect_metrics_functions_script" {
  name   = "${var.functions_scripts_folder}start_collect_metrics.zip"
  bucket = var.bucket_name
  source = "${path.module}/zips/start_collect_metrics.zip"
  depends_on = [null_resource.zip_scripts]
}