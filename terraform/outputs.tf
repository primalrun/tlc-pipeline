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

output "pipeline_role_arn" {
  description = "ARN of the IAM role for the pipeline"
  value       = aws_iam_role.pipeline.arn
}
