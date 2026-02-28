data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "pipeline_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${var.project_name}-pipeline-role"
  assume_role_policy = data.aws_iam_policy_document.pipeline_assume_role.json
}

data "aws_iam_policy_document" "pipeline_s3" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
      aws_s3_bucket.staging.arn,
      "${aws_s3_bucket.staging.arn}/*",
      aws_s3_bucket.processed.arn,
      "${aws_s3_bucket.processed.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "pipeline_s3" {
  name   = "${var.project_name}-s3-access"
  role   = aws_iam_role.pipeline.id
  policy = data.aws_iam_policy_document.pipeline_s3.json
}

# IAM role for Snowflake storage integration
# Phase 1: trust policy uses account root as placeholder
# Phase 2: updated to trust Snowflake's IAM user once integration outputs are known
data "aws_iam_policy_document" "snowflake_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "AWS"
      identifiers = var.snowflake_iam_user_arn != "" ? [var.snowflake_iam_user_arn] : [
        "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
      ]
    }
    dynamic "condition" {
      for_each = var.snowflake_external_id != "" ? [1] : []
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [var.snowflake_external_id]
      }
    }
  }
}

resource "aws_iam_role" "snowflake" {
  name               = "${var.project_name}-snowflake-role"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume.json
}

data "aws_iam_policy_document" "snowflake_s3" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.processed.arn,
      "${aws_s3_bucket.processed.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "snowflake_s3" {
  name   = "${var.project_name}-snowflake-s3-access"
  role   = aws_iam_role.snowflake.id
  policy = data.aws_iam_policy_document.snowflake_s3.json
}
