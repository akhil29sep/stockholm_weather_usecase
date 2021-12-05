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

resource "aws_iam_role" "glue" {
  name = "AWSGlueServiceRoleDefault"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}
resource "aws_iam_role_policy" "my_s3_policy" {
  name = "my_s3_policy"
  role = "${aws_iam_role.glue.id}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::${aws_s3_bucket.create_bucket.id}",
        "arn:aws:s3:::${aws_s3_bucket.create_bucket.id}/*"
      ]
    }
  ]
}
EOF
}


resource "aws_glue_job" "akhil_test_job" {
  name     = "akhil_test_job"
  role_arn = "${aws_iam_role.glue.id}"

  command {
    script_location = "s3://${aws_s3_bucket.create_bucket.id}/pyspark_Code/pyscript.py"
  }
}

resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  name = "akhil_test_db"
  location_uri = ""
}


