# STEP 4: the wiring diagram. Each module block = one layer; read top-to-bottom = creation order.
locals {
  name = "${var.project}-${var.environment}"
  azs  = slice(data.aws_availability_zones.available.names, 0, var.az_count)
}

data "aws_availability_zones" "available" { state = "available" }
data "aws_route53_zone" "this" {
  name         = var.zone_name
  private_zone = false
}

# L0: network (VPC, public + private subnets, NAT)
module "network" {
  source             = "./modules/network"
  name               = local.name
  vpc_cidr           = var.vpc_cidr
  azs                = local.azs
  single_nat_gateway = var.single_nat_gateway
}

# Container registries (one per service)
module "ecr" {
  source = "./modules/ecr"
  name   = var.project
}

# L2 compute: EKS in PRIVATE subnets
module "eks" {
  source              = "./modules/eks"
  name                = local.name
  cluster_version     = var.eks_version
  vpc_id              = module.network.vpc_id
  subnet_ids          = module.network.private_subnet_ids
  node_instance_types = var.node_instance_types
  node_min            = var.node_min
  node_max            = var.node_max
  node_desired        = var.node_desired
}

# Data plane: RDS x3, ElastiCache x2, MSK. Only the EKS nodes may connect (security groups).
module "data" {
  source                    = "./modules/data"
  name                      = local.name
  vpc_id                    = module.network.vpc_id
  subnet_ids                = module.network.private_subnet_ids
  client_security_group_id  = module.eks.node_security_group_id
  db_instance_class         = var.db_instance_class
  redis_node_type           = var.redis_node_type
  msk_instance_type         = var.msk_instance_type
  multi_az                  = var.multi_az
  deletion_protection       = var.deletion_protection
}

# L1 edge: Route 53 + CloudFront + WAF + Shield + external ALB (public subnets)
module "edge" {
  source = "./modules/edge"
  providers = {
    aws      = aws
    aws.use1 = aws.use1
  }
  name                      = local.name
  vpc_id                    = module.network.vpc_id
  public_subnet_ids         = module.network.public_subnet_ids
  node_security_group_id    = module.eks.node_security_group_id
  zone_id                   = data.aws_route53_zone.this.zone_id
  zone_name                 = var.zone_name
  api_hostname              = "${var.api_subdomain}.${var.zone_name}"
  origin_hostname           = "${var.origin_subdomain}.${var.zone_name}"
  waf_rate_limit_per_5min   = var.waf_rate_limit_per_5min
  enable_shield_advanced    = var.enable_shield_advanced
  deletion_protection       = var.deletion_protection
}
