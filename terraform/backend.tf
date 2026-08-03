terraform {
  backend "s3" {
    bucket  = "flight-delay-terraform-state-851725344261"
    key     = "flight-delay/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}
