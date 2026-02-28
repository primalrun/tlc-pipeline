resource "snowflake_warehouse" "tlc" {
  name           = "TLC_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_database" "tlc" {
  name = "TLC"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.tlc.name
  name     = "RAW"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.tlc.name
  name     = "STAGING"
}

resource "snowflake_schema" "mart" {
  database = snowflake_database.tlc.name
  name     = "MART"
}

resource "snowflake_account_role" "transform" {
  name = "TRANSFORM_ROLE"
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.transform.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.tlc.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.transform.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.tlc.name
  }
}

# Storage integration for S3 external stage
resource "snowflake_storage_integration" "s3" {
  name    = "TLC_S3_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider     = "S3"
  storage_aws_role_arn = aws_iam_role.snowflake.arn

  storage_allowed_locations = ["s3://tlc-pipeline-processed/"]
}

# External stage — created in phase 2 once trust policy is updated
resource "snowflake_stage" "processed" {
  count    = var.snowflake_iam_user_arn != "" ? 1 : 0
  name     = "TLC_PROCESSED_STAGE"
  database = snowflake_database.tlc.name
  schema   = snowflake_schema.raw.name
  url      = "s3://tlc-pipeline-processed/yellow_tripdata"

  storage_integration = snowflake_storage_integration.s3.name
}
