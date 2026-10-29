terraform {
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.60", configuration_aliases = [aws.use1] }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
}
