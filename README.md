# Meu Repositório GCP Terraform

Este repositório contém os arquivos necessários para automatizar a implantação de infraestrutura no Google Cloud Platform (GCP) usando Terraform. Também inclui fluxos de trabalho para pipelines de processamento de dados usando GitHub Actions.

## Configuração do Terraform

O arquivo `main.tf` define a configuração do Terraform. Inclui o seguinte:

- Configuração para o provedor GCP.
- Ativação das APIs necessárias (Dataproc, BigQuery, Storage).
- Definição dos recursos necessários, como buckets do Cloud Storage, contas de serviço e permissões.
- Configuração do backend do Terraform para usar um bucket GCS existente para armazenar o estado (`terraform.tfstate`).

O arquivo `variables.tf` declara as variáveis necessárias:

```hcl
variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "dataproc_service_account" {
  type = string
}
```