# AWS-managed policies (fixed, well-known ids)
locals {
  cache_policy_caching_disabled      = "4135ea2d-6df8-44a3-9df3-4b5a748f1efd" # Managed-CachingDisabled
  origin_request_all_viewer_no_host  = "b689b0a8-53d0-40ab-baf2-68738e2966ac" # Managed-AllViewerExceptHostHeader
}

resource "aws_cloudfront_distribution" "api" {
  enabled         = true
  is_ipv6_enabled = true
  comment         = "${var.name} API edge"
  aliases         = [var.api_hostname]
  http_version    = "http2and3"
  price_class     = "PriceClass_200"
  web_acl_id      = aws_wafv2_web_acl.edge.arn          # WAF attached here

  origin {
    origin_id   = "alb"
    domain_name = var.origin_hostname                    # must match the ALB certificate (origin.<zone>)
    custom_origin_config {
      http_port                = 80
      https_port               = 443
      origin_protocol_policy   = "https-only"
      origin_ssl_protocols     = ["TLSv1.2"]
      origin_read_timeout      = 30
    }
    custom_header {
      name  = "X-Origin-Verify"
      value = random_password.origin_verify.result
    }
  }

  # An API is dynamic and per-user => NO caching. CloudFront still gives: TLS termination near the user,
  # AWS backbone to the origin, WAF/Shield enforcement at the edge, absorbing connection floods.
  default_cache_behavior {
    target_origin_id         = "alb"
    viewer_protocol_policy   = "redirect-to-https"
    allowed_methods          = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods           = ["GET", "HEAD"]
    cache_policy_id          = local.cache_policy_caching_disabled
    origin_request_policy_id = local.origin_request_all_viewer_no_host   # forwards Authorization + all headers
    compress                 = true
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.cloudfront.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}
