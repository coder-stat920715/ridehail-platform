variable "name"               { type = string }
variable "vpc_cidr"           { type = string }
variable "azs"                { type = list(string) }
variable "single_nat_gateway" { type = bool }

# Public subnets (/24)  : external ALB + NAT gateways           -> internet-facing tier (L1)
# Private subnets (/20) : EKS nodes/pods, RDS, Redis, MSK        -> never directly reachable (L2)
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = var.name
  cidr = var.vpc_cidr
  azs  = var.azs

  public_subnets  = [for i, _ in var.azs : cidrsubnet(var.vpc_cidr, 8, i)]
  private_subnets = [for i, _ in var.azs : cidrsubnet(var.vpc_cidr, 4, i + 1)]

  enable_nat_gateway     = true
  single_nat_gateway     = var.single_nat_gateway
  one_nat_gateway_per_az = !var.single_nat_gateway
  enable_dns_hostnames   = true

  # Tags the AWS Load Balancer Controller uses to discover subnets.
  public_subnet_tags  = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = { "kubernetes.io/role/internal-elb" = 1 }
}

output "vpc_id"             { value = module.vpc.vpc_id }
output "public_subnet_ids"  { value = module.vpc.public_subnets }
output "private_subnet_ids" { value = module.vpc.private_subnets }
