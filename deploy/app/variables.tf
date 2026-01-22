variable "app" {
  description = "The name of the application"
  type        = string
}

variable "allowed_account_id" {
  description = "account id used for AWS"
  type = string
}

variable "region" {
  description = "aws region for all services"
  type        = string
}

variable "private_key" {
  description = "private_key for the peer for this deployment"
  type = string
}

variable "did" {
  description = "DID for this deployment (did:web:... for example)"
  type = string
}

variable "image_tag" {
  description = "ECR image tag to deploy with"
  type = string
}

variable "principal_mapping" {
  type        = string
  description = "JSON encoded mapping of did:web to did:key"
  default     = ""
}

variable "env_files" {
  description = "list of environment variable files to upload"
  type = list(string)
  default = []
}

variable "domain_base" {
  type = string
  default = ""
}

variable "network" {
  description = "The network to use (defaults to the default 'hot' network)"
  type        = string
  default     = "hot"
}

variable "metrics_auth_token" {
  description = "value for metrics_auth_token secret"
  type = string
}

variable "admin_dashboard_user" {
  description = "value for admin_dashboard_user secret"
  type = string
}

variable "admin_dashboard_password" {
  description = "value for admin_dashboard_password secret"
  type = string
}


variable "client_egress_dollars_per_tib" {
  description = "Client egress cost in dollars per TiB"
  type        = string
}

variable "provider_egress_dollars_per_tib" {
  description = "Provider egress rate in dollars per TiB"
  type        = string
}