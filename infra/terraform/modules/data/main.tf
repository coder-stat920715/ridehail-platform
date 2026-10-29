variable "name"                     { type = string }
variable "vpc_id"                   { type = string }
variable "subnet_ids"               { type = list(string) }
variable "client_security_group_id" { type = string }
variable "db_instance_class"        { type = string }
variable "redis_node_type"          { type = string }
variable "msk_instance_type"        { type = string }
variable "multi_az"                 { type = bool }
variable "deletion_protection"      { type = bool }

# ---------------------------------------------------------------- security groups (only EKS nodes may connect)
resource "aws_security_group" "rds" {
  name_prefix = "${var.name}-rds-"
  vpc_id      = var.vpc_id
  ingress {
    description     = "PostgreSQL from EKS nodes"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.client_security_group_id]
  }
}
resource "aws_security_group" "redis" {
  name_prefix = "${var.name}-redis-"
  vpc_id      = var.vpc_id
  ingress {
    description     = "Redis from EKS nodes"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [var.client_security_group_id]
  }
}
resource "aws_security_group" "msk" {
  name_prefix = "${var.name}-msk-"
  vpc_id      = var.vpc_id
  ingress {
    description     = "Kafka TLS from EKS nodes"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [var.client_security_group_id]
  }
}

# ---------------------------------------------------------------- PostgreSQL: DATABASE PER SERVICE
locals {
  databases = {
    trip         = "tripdb"
    location     = "locationdb"
    payment      = "paymentdb"
    matching     = "matchingdb"
    notification = "notificationdb"
  }
}

resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-db"
  subnet_ids = var.subnet_ids
}

# Debezium reads the WAL => logical replication MUST be on (static parameters => applied after reboot).
resource "aws_db_parameter_group" "pg16_logical" {
  name_prefix = "${var.name}-pg16-"
  family      = "postgres16"
  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "max_replication_slots"
    value        = "10"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "max_wal_senders"
    value        = "10"
    apply_method = "pending-reboot"
  }
  lifecycle { create_before_destroy = true }
}

resource "aws_db_instance" "this" {
  for_each = local.databases

  identifier        = "${var.name}-${each.key}"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = var.db_instance_class
  allocated_storage     = 20
  max_allocated_storage = 200
  storage_encrypted     = true

  db_name                     = each.value
  username                    = "ridehail_admin"
  manage_master_user_password = true            # password generated + stored in Secrets Manager (never in state/tfvars)

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.pg16_logical.name
  publicly_accessible    = false

  multi_az                     = var.multi_az
  backup_retention_period      = 7
  performance_insights_enabled = true
  auto_minor_version_upgrade   = true
  deletion_protection          = var.deletion_protection
  skip_final_snapshot          = !var.deletion_protection
}

# ---------------------------------------------------------------- Redis: one group for the gateway limiter, one for the geo read model
resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.name}-redis"
  subnet_ids = var.subnet_ids
}

resource "aws_elasticache_replication_group" "this" {
  for_each = toset(["limiter", "geo"])

  replication_group_id       = "${var.name}-${each.key}"
  description                = "ridehail ${each.key} redis"
  engine                     = "redis"
  engine_version             = "7.1"
  node_type                  = var.redis_node_type
  num_cache_clusters         = 2
  automatic_failover_enabled = true
  multi_az_enabled           = true
  port                       = 6379
  subnet_group_name          = aws_elasticache_subnet_group.this.name
  security_group_ids         = [aws_security_group.redis.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true            # clients must use TLS: SPRING_DATA_REDIS_SSL_ENABLED=true
}

# ---------------------------------------------------------------- Kafka (MSK)
resource "aws_msk_configuration" "this" {
  name           = "${var.name}-kafka"
  kafka_versions = ["3.6.0"]
  server_properties = <<-EOT
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=6
    unclean.leader.election.enable=false
  EOT
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.name}-kafka"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = length(var.subnet_ids)           # one broker per AZ

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.msk.id]
    storage_info { ebs_storage_info { volume_size = 100 } }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
  client_authentication { unauthenticated = true }   # network-isolated via SG; harden to IAM/SASL for prod
}

output "rds_endpoints"           { value = { for k, d in aws_db_instance.this : k => d.address } }
output "rds_master_secret_arns"  { value = { for k, d in aws_db_instance.this : k => d.master_user_secret[0].secret_arn } }
output "redis_endpoints"         { value = { for k, r in aws_elasticache_replication_group.this : k => r.primary_endpoint_address } }
output "msk_bootstrap_brokers_tls" { value = aws_msk_cluster.this.bootstrap_brokers_tls }
