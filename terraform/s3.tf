resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw"
}

resource "aws_s3_bucket" "staging" {
  bucket = "${var.project_name}-staging"
}

resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-processed"
}
