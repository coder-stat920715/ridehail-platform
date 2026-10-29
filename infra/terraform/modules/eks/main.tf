variable "name"                { type = string }
variable "cluster_version"     { type = string }
variable "vpc_id"              { type = string }
variable "subnet_ids"          { type = list(string) }
variable "node_instance_types" { type = list(string) }
variable "node_min"            { type = number }
variable "node_max"            { type = number }
variable "node_desired"        { type = number }

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = var.name
  cluster_version = var.cluster_version
  vpc_id          = var.vpc_id
  subnet_ids      = var.subnet_ids              # control-plane ENIs + nodes in PRIVATE subnets

  cluster_endpoint_public_access           = true   # restrict with cluster_endpoint_public_access_cidrs in prod
  enable_cluster_creator_admin_permissions = true
  enable_irsa                              = true   # pods assume IAM roles via service accounts (no node-wide creds)

  cluster_addons = {
    coredns    = {}
    kube-proxy = {}
    vpc-cni    = {}
  }

  eks_managed_node_groups = {
    default = {
      instance_types = var.node_instance_types
      min_size       = var.node_min
      max_size       = var.node_max
      desired_size   = var.node_desired
    }
  }
}

output "cluster_name"            { value = module.eks.cluster_name }
output "cluster_endpoint"        { value = module.eks.cluster_endpoint }
output "cluster_ca_data"         { value = module.eks.cluster_certificate_authority_data }
output "node_security_group_id"  { value = module.eks.node_security_group_id }
output "oidc_provider_arn"       { value = module.eks.oidc_provider_arn }
