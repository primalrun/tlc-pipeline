output "s3_bucket_raw_arn" {
  description = "ARN of the raw data S3 bucket"
  value       = aws_s3_bucket.raw.arn
}

output "s3_bucket_staging_arn" {
  description = "ARN of the staging data S3 bucket"
  value       = aws_s3_bucket.staging.arn
}

output "s3_bucket_processed_arn" {
  description = "ARN of the processed data S3 bucket"
  value       = aws_s3_bucket.processed.arn
}

output "snowflake_warehouse_name" {
  description = "Name of the Snowflake warehouse"
  value       = snowflake_warehouse.tlc.name
}

output "snowflake_integration_iam_user_arn" {
  description = "Snowflake IAM user ARN — add to terraform.tfvars as snowflake_iam_user_arn"
  value       = snowflake_storage_integration.s3.storage_aws_iam_user_arn
}

output "snowflake_integration_external_id" {
  description = "Snowflake external ID — add to terraform.tfvars as snowflake_external_id"
  value       = snowflake_storage_integration.s3.storage_aws_external_id
}

output "pipeline_role_arn" {
  description = "ARN of the IAM role for the pipeline"
  value       = aws_iam_role.pipeline.arn
}
