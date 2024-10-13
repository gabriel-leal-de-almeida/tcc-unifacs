variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "dataproc_scripts_folder" {
  description = "Path local para os scripts Python que serão utilizados no Dataproc"
  default     = "scripts/dataproc/"
}

variable "functions_scripts_folder" {
  description = "Path local para os scripts Python que serão utilizados no Cloud Functions"
  default     = "scripts/functions/"
}