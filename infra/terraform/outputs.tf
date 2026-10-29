# STEP 6: what you need after `apply` (feed these into k8s/ and your CI).
output "api_url"                 { value = "https://${module.edge.api_hostname}" }
output "cloudfront_domain"       { value = module.edge.cloudfront_domain_name }
output "alb_target_group_arn" {
  value = module.edge.target_group_arn
  description = "Paste into k8s/base/targetgroupbinding.yaml"
}
output "ecr_repository_urls"     { value = module.ecr.repository_urls }
output "eks_cluster_name"        { value = module.eks.cluster_name }
output "kubeconfig_command"      { value = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}" }
output "rds_endpoints"           { value = module.data.rds_endpoints }
output "rds_master_secret_arns"  { value = module.data.rds_master_secret_arns }
output "redis_endpoints"         { value = module.data.redis_endpoints }
output "msk_bootstrap_brokers_tls" { value = module.data.msk_bootstrap_brokers_tls }
