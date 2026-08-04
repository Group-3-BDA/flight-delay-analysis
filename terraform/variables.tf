variable "aws_region" {
  description = "AWS Region"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM Role ARN used by AWS Glue"
  type        = string
}

variable "gold_bucket_name" {
  description = "Gold S3 bucket containing analytics datasets"
  type        = string
}
