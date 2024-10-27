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

data "archive_file" "start_process_data_functions_script_zip" {
  type        = "zip"
  source_dir  = "${path.module}/${var.functions_scripts_folder}/start_process_data"
  output_path = "${path.module}/zips/start_process_data.zip"
}

data "archive_file" "start_read_data_functions_script_zip" {
  type        = "zip"
  source_dir  = "${path.module}/${var.functions_scripts_folder}/start_read_data"
  output_path = "${path.module}/zips/start_read_data.zip"
}

data "archive_file" "start_collect_metrics_functions_script_zip" {
  type        = "zip"
  source_dir  = "${path.module}/${var.functions_scripts_folder}/start_collect_metrics"
  output_path = "${path.module}/zips/start_collect_metrics.zip"
}

# Upload zipped scripts to GCS
resource "google_storage_bucket_object" "start_process_data_functions_script" {
  name   = "${var.functions_scripts_folder}start_process_data.zip"
  bucket = var.bucket_name
  source = "${path.module}/zips/start_process_data.zip"
  depends_on = [data.archive_file.start_process_data_functions_script_zip]
}

resource "google_storage_bucket_object" "start_read_data_functions_script" {
  name   = "${var.functions_scripts_folder}start_read_data.zip"
  bucket = var.bucket_name
  source = "${path.module}/zips/start_read_data.zip"
  depends_on = [data.archive_file.start_read_data_functions_script_zip]
}

resource "google_storage_bucket_object" "start_collect_metrics_functions_script" {
  name   = "${var.functions_scripts_folder}start_collect_metrics.zip"
  bucket = var.bucket_name
  source = "${path.module}/zips/start_collect_metrics.zip"
  depends_on = [data.archive_file.start_collect_metrics_functions_script_zip]
}

data "archive_file" "start_write_bigquery_functions_script_zip" {
  type        = "zip"
  source_dir  = "${path.module}/${var.functions_scripts_folder}/start_write_bigquery"
  output_path = "${path.module}/zips/start_write_bigquery.zip"
}

resource "google_storage_bucket_object" "start_write_bigquery_functions_script" {
  name   = "${var.functions_scripts_folder}start_write_bigquery.zip"
  bucket = var.bucket_name
  source = "${path.module}/zips/start_write_bigquery.zip"
  depends_on = [data.archive_file.start_write_bigquery_functions_script_zip]
}

# Baixe o JAR do Maven usando um recurso local_file
resource "local_file" "download_spark_avro_jar" {
  filename = "${path.module}/tmp/spark-avro_2.13-3.5.1.jar"
  content  = filebase64("https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/3.5.1/spark-avro_2.13-3.5.1.jar")
}

# Carregue o JAR para o bucket do GCS
resource "google_storage_bucket_object" "spark_avro_jar" {
  name   = "libs/jars/spark-avro_2.13-3.5.1.jar"
  bucket = var.bucket_name
  source = local_file.download_spark_avro_jar.filename

  depends_on = [local_file.download_spark_avro_jar]
}