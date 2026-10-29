# Shield STANDARD is automatic and free. Shield ADVANCED is a paid subscription (1-year term) that adds:
# L7 DDoS detection with automatic WAF mitigation, DRT support, cost protection and attack metrics.
# Terraform only attaches PROTECTIONS; the subscription itself must exist (console, or aws_shield_subscription).
resource "aws_shield_protection" "cloudfront" {
  count        = var.enable_shield_advanced ? 1 : 0
  name         = "${var.name}-cloudfront"
  resource_arn = aws_cloudfront_distribution.api.arn
}

resource "aws_shield_protection" "alb" {
  count        = var.enable_shield_advanced ? 1 : 0
  name         = "${var.name}-alb"
  resource_arn = aws_lb.external.arn
}

resource "aws_shield_protection" "route53_zone_health" {
  count        = var.enable_shield_advanced ? 1 : 0
  name         = "${var.name}-r53-healthcheck"
  resource_arn = "arn:aws:route53:::hostedzone/${var.zone_id}"
}
