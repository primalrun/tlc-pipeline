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
