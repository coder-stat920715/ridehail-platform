# STEP 3: every knob. Values come from terraform.tfvars (see terraform.tfvars.example).
variable "project" {
  type = string
  default = "ridehail"
}
variable "environment" {
  type = string
  description = "dev | staging | prod"
}
variable "region" {
  type = string
  default = "ap-south-1"
}

# ---- DNS / edge
variable "zone_name" {
  type = string
  description = "Existing Route 53 public hosted zone, e.g. example.com"
}
variable "api_subdomain" {
  type = string
  default = "api"
  description = "Public hostname users call: api.<zone>"
}
variable "origin_subdomain" {
  type = string
  default = "origin"
  description = "Hostname of the ALB (CloudFront -> origin): origin.<zone>"
}
variable "waf_rate_limit_per_5min" {
  type = number
  default = 2000
  description = "Max requests per IP per 5 minutes before WAF blocks"
}
variable "enable_shield_advanced" {
  type = bool
  default = false
  description = "Requires an active Shield Advanced subscription (paid, 1-year commitment)"
}

# ---- network
variable "vpc_cidr" {
  type = string
  default = "10.0.0.0/16"
}
variable "az_count" {
  type = number
  default = 3
}
variable "single_nat_gateway" {
  type = bool
  default = true
  description = "true = cheap (dev); false = one NAT per AZ (prod)"
}

# ---- compute
variable "eks_version" {
  type = string
  default = "1.30"
}
variable "node_instance_types" {
  type = list(string)
  default = ["m6i.large"]
}
variable "node_min" {
  type = number
  default = 3
}
variable "node_max" {
  type = number
  default = 9
}
variable "node_desired" {
  type = number
  default = 3
}

# ---- data
variable "db_instance_class" {
  type = string
  default = "db.t4g.medium"
}
variable "redis_node_type" {
  type = string
  default = "cache.t4g.small"
}
variable "msk_instance_type" {
  type = string
  default = "kafka.t3.small"
}
variable "multi_az" {
  type = bool
  default = false
  description = "RDS Multi-AZ (true for prod)"
}
variable "deletion_protection" {
  type = bool
  default = false
  description = "true for prod"
}
