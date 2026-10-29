# Two certificates, because CloudFront only accepts certs from us-east-1 while the ALB needs one in its own region.
#   api.<zone>    -> viewers talk to CloudFront   (cert in us-east-1)
#   origin.<zone> -> CloudFront talks to the ALB  (cert in the app region)
resource "aws_acm_certificate" "cloudfront" {
  provider          = aws.use1
  domain_name       = var.api_hostname
  validation_method = "DNS"
  lifecycle { create_before_destroy = true }
}

resource "aws_acm_certificate" "alb" {
  domain_name       = var.origin_hostname
  validation_method = "DNS"
  lifecycle { create_before_destroy = true }
}

# Terraform itself creates the DNS records ACM asks for, then waits until the certificates are ISSUED.
resource "aws_route53_record" "cert_validation" {
  for_each = {
    for o in concat(aws_acm_certificate.cloudfront.domain_validation_options, aws_acm_certificate.alb.domain_validation_options) :
    o.domain_name => { name = o.resource_record_name, type = o.resource_record_type, value = o.resource_record_value }
  }
  zone_id         = var.zone_id
  name            = each.value.name
  type            = each.value.type
  records         = [each.value.value]
  ttl             = 60
  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "cloudfront" {
  provider                = aws.use1
  certificate_arn         = aws_acm_certificate.cloudfront.arn
  validation_record_fqdns = [aws_route53_record.cert_validation[var.api_hostname].fqdn]
}

resource "aws_acm_certificate_validation" "alb" {
  certificate_arn         = aws_acm_certificate.alb.arn
  validation_record_fqdns = [aws_route53_record.cert_validation[var.origin_hostname].fqdn]
}
