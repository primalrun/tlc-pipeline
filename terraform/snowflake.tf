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

resource "snowflake_role" "transform" {
  name = "TRANSFORM_ROLE"
}

resource "snowflake_grant_privileges_to_role" "warehouse_usage" {
  privileges = ["USAGE"]
  role_name  = snowflake_role.transform.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.tlc.name
  }
}

resource "snowflake_grant_privileges_to_role" "database_usage" {
  privileges = ["USAGE"]
  role_name  = snowflake_role.transform.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.tlc.name
  }
}
