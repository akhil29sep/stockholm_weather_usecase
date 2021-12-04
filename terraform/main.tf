provider "aws" {
  region = "eu-west-1"
}

# create bucket
resource "aws_s3_bucket" "create_bucket" {
  bucket = "eu-dev-akhil-data"
  acl    = "private"

  tags = {
    Name        = "Akhil"
    Environment = "Dev"
  }
}

# Blocked all public access to s3 bucket
resource "aws_s3_bucket_public_access_block" "block_access" {
  bucket = aws_s3_bucket.create_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Sync data from local to s3 location
resource "null_resource" "remove_and_upload_to_s3" {
  provisioner "local-exec" {
    command = "aws s3 sync ${path.module}/../stockholm_weather s3://${aws_s3_bucket.create_bucket.id}"
  }
}
