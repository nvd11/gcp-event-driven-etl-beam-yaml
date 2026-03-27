resource "google_bigquery_dataset" "etl_dataset" {
  dataset_id                 = "etl_poc_dataset"
  friendly_name              = "ETL PoC Dataset"
  description                = "Dataset for event driven ETL PoC (Beam YAML)"
  location                   = var.region
  delete_contents_on_destroy = true
}

# 目标表可以由 Terraform 预先建好，也可以由 Dataflow YAML CREATE_IF_NEEDED 自动创建。
# 为了保持基础设施的完备性，这里给出一个可选的目标表的定义。
resource "google_bigquery_table" "target_table" {
  dataset_id          = google_bigquery_dataset.etl_dataset.dataset_id
  table_id            = "target_users_poc"
  deletion_protection = false

  schema = <<EOF
[
  {
    "name": "id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "email",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "role",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF
}
