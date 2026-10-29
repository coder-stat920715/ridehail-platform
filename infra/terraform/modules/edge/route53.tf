# api.<zone>    -> CloudFront   (what users/mobile apps call)
resource "aws_route53_record" "api_a" {
  zone_id = var.zone_id
  name    = var.api_hostname
  type    = "A"
  alias {
    name                   = aws_cloudfront_distribution.api.domain_name
    zone_id                = aws_cloudfront_distribution.api.hosted_zone_id
    evaluate_target_health = false
  }
}
resource "aws_route53_record" "api_aaaa" {
  zone_id = var.zone_id
  name    = var.api_hostname
  type    = "AAAA"
  alias {
    name                   = aws_cloudfront_distribution.api.domain_name
    zone_id                = aws_cloudfront_distribution.api.hosted_zone_id
    evaluate_target_health = false
  }
}

# origin.<zone> -> ALB (only CloudFront uses this name; the ALB rejects requests without the secret header)
resource "aws_route53_record" "origin" {
  zone_id = var.zone_id
  name    = var.origin_hostname
  type    = "A"
  alias {
    name                   = aws_lb.external.dns_name
    zone_id                = aws_lb.external.zone_id
    evaluate_target_health = true
  }
}

# Health check from Route 53 checkers against the public endpoint (through CloudFront -> ALB -> gateway).
# Use it for alarms now, and as failover/latency routing health in a multi-region setup.
resource "aws_route53_health_check" "api" {
  fqdn              = var.api_hostname
  type              = "HTTPS"
  port              = 443
  resource_path     = "/actuator/health/liveness"
  request_interval  = 30
  failure_threshold = 3
  measure_latency   = true
  tags              = { Name = "${var.name}-api" }
}
