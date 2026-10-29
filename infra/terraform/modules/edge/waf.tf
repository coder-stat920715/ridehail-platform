# Web ACL attached to the CloudFront distribution. Rules run in priority order; the first terminating match wins.
resource "aws_wafv2_web_acl" "edge" {
  provider    = aws.use1
  name        = "${var.name}-edge"
  description = "ridehail edge protection"
  scope       = "CLOUDFRONT"

  default_action {
    allow {}
  }

  # 1) Per-IP rate limit: > N requests / 5 minutes => block. Hard outer limit; the gateway enforces finer per-USER limits.
  rule {
    name     = "rate-limit-per-ip"
    priority = 1
    action {
      block {}
    }
    statement {
      rate_based_statement {
        limit              = var.waf_rate_limit_per_5min
        aggregate_key_type = "IP"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "rate-limit-per-ip"
      sampled_requests_enabled   = true
    }
  }

  # 2..5) AWS managed rule groups (override_action none = use each rule's own block action)
  dynamic "rule" {
    for_each = {
      2 = "AWSManagedRulesAmazonIpReputationList"
      3 = "AWSManagedRulesCommonRuleSet"          # XSS, path traversal, bad bots, oversized bodies ...
      4 = "AWSManagedRulesSQLiRuleSet"            # SQL injection
      5 = "AWSManagedRulesKnownBadInputsRuleSet"  # Log4Shell and other known exploit payloads
    }
    content {
      name     = rule.value
      priority = tonumber(rule.key)
      override_action {
        none {}
      }
      statement {
        managed_rule_group_statement {
          name        = rule.value
          vendor_name = "AWS"
        }
      }
      visibility_config {
        cloudwatch_metrics_enabled = true
        metric_name                = rule.value
        sampled_requests_enabled   = true
      }
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${var.name}-edge"
    sampled_requests_enabled   = true
  }
}
