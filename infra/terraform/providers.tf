# STEP 2: how Terraform authenticates and which regions it talks to.
provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = var.project, Environment = var.environment, ManagedBy = "terraform" }
  }
}

# CloudFront certificates and CLOUDFRONT-scope WAF web ACLs MUST live in us-east-1, whatever region the app uses.
provider "aws" {
  alias  = "use1"
  region = "us-east-1"
  default_tags {
    tags = { Project = var.project, Environment = var.environment, ManagedBy = "terraform" }
  }
}

# Helm talks to the EKS cluster created below (used only to install the AWS Load Balancer Controller).
provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_ca_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
    }
  }
}
