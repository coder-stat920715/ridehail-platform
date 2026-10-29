# STEP 1 of reading: which Terraform / provider versions, and where the state lives.
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.60" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
    helm   = { source = "hashicorp/helm", version = "~> 2.14" }
  }

  # Remote state (S3 + DynamoDB lock). Values are supplied at init time so no account ids live in git:
  #   terraform init -backend-config=backend.hcl        (see backend.hcl.example)
  backend "s3" {}
}
