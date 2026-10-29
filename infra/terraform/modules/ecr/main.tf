variable "name" { type = string }

locals {
  services = ["eureka-server", "gateway-service", "trip-service", "location-service", "payment-service", "matching-service", "notification-service"]
}

resource "aws_ecr_repository" "this" {
  for_each             = toset(local.services)
  name                 = "${var.name}/${each.key}"
  image_tag_mutability = "IMMUTABLE"             # a tag (e.g. git sha) can never be silently overwritten
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }
}

resource "aws_ecr_lifecycle_policy" "this" {
  for_each   = aws_ecr_repository.this
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 20 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 20 }
      action       = { type = "expire" }
    }]
  })
}

output "repository_urls" { value = { for k, r in aws_ecr_repository.this : k => r.repository_url } }
