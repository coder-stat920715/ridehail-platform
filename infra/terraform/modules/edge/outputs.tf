output "api_hostname" {
  value = var.api_hostname
}
output "cloudfront_domain_name" {
  value = aws_cloudfront_distribution.api.domain_name
}
output "target_group_arn" {
  value = aws_lb_target_group.gateway.arn
}
output "alb_dns_name" {
  value = aws_lb.external.dns_name
}
