variable "name" {
  type = string
}
variable "vpc_id" {
  type = string
}
variable "public_subnet_ids" {
  type = list(string)
}
variable "node_security_group_id" {
  type = string
}
variable "zone_id" {
  type = string
}
variable "zone_name" {
  type = string
}
variable "api_hostname" {
  type = string
}
variable "origin_hostname" {
  type = string
}
variable "waf_rate_limit_per_5min" {
  type = number
}
variable "enable_shield_advanced" {
  type = bool
}
variable "deletion_protection" {
  type = bool
}
