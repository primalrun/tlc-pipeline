variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "tlc-pipeline"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "snowflake_org" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account name (without org prefix)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake admin user"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake admin password"
  type        = string
  sensitive   = true
}

variable "snowflake_iam_user_arn" {
  description = "Snowflake IAM user ARN from storage integration (fill in after phase 1 apply)"
  type        = string
  default     = ""
}

variable "snowflake_external_id" {
  description = "Snowflake external ID from storage integration (fill in after phase 1 apply)"
  type        = string
  default     = ""
}
