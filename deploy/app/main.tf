terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0.0"
    }
    archive = {
      source = "hashicorp/archive"
    }
  }
  backend "s3" {
    bucket = "storacha-terraform-state"
    key = "storacha/${var.app}/terraform.tfstate"
    region = "us-west-2"
    encrypt = true
  }
}

provider "aws" {
  allowed_account_ids = [var.allowed_account_id]
  region = var.region
  default_tags {
    tags = {
      "Environment" = terraform.workspace
      "ManagedBy"   = "OpenTofu"
      Owner         = "storacha"
      Team          = "Storacha Engineering"
      Organization  = "Storacha"
      Project       = "${var.app}"
    }
  }
}



module "app" {
  source = "github.com/storacha/storoku//app?ref=v0.6.2"
  private_key = var.private_key
  private_key_env_var = "ETRACKER_PRIVATE_KEY"
  httpport = 8080
  principal_mapping = var.principal_mapping
  did = var.did
  did_env_var = "ETRACKER_DID"
  app = var.app
  appState = var.app
  write_to_container = false
  environment = terraform.workspace
  network = var.network
  # if there are any env vars you want available only to your container
  # in the vpc as opposed to set in the dockerfile, enter them here
  # NOTE: do not put sensitive data in env-vars. use secrets
  deployment_env_vars = [
    {
      name = "ETRACKER_CLIENT_EGRESS_USD_PER_TIB"
      value = var.client_egress_usd_per_tib
    },
    {
      name = "ETRACKER_PROVIDER_EGRESS_USD_PER_TIB"
      value = var.provider_egress_usd_per_tib
    }
  ]
  image_tag = var.image_tag
  create_db = false
  # enter secret values your app will use here -- these will be available
  # as env vars in the container at runtime
  secrets = { 
    "ETRACKER_METRICS_AUTH_TOKEN" = var.metrics_auth_token
    "ETRACKER_ADMIN_DASHBOARD_USER" = var.admin_dashboard_user
    "ETRACKER_ADMIN_DASHBOARD_PASSWORD" = var.admin_dashboard_password
  }
  # enter external secrets (provisioned out-of-band) here
  external_secrets = []
  # enter any sqs queues you want to create here
  queues = []
  caches = []
  topics = []
  tables = [
    {
      name = "egress-records"
      attributes = [
        {
          name = "batch"
          type = "S"
        },
        {
          name = "unprocessedSince"
          type = "S"
        },
      ]
      hash_key = "batch"
      global_secondary_indexes = [
        {
          name = "unprocessed"
          hash_key = "batch"
          range_key = "unprocessedSince"
          projection_type = "INCLUDE"
          non_key_attributes = ["node","cause",]
        },
      ]
    },
    {
      name = "consolidated-records"
      attributes = [
        {
          name = "cause"
          type = "S"
        },
        {
          name = "node"
          type = "S"
        },
        {
          name = "processedAt"
          type = "S"
        },
      ]
      hash_key = "cause"
      global_secondary_indexes = [
        {
          name = "node-stats"
          hash_key = "node"
          range_key = "processedAt"
          projection_type = "INCLUDE"
          non_key_attributes = ["totalEgress",]
        },
      ]
    },
    {
      name = "space-stats"
      attributes = [
        {
          name = "space"
          type = "S"
        },
        {
          name = "date"
          type = "S"
        },
      ]
      hash_key = "space"
      range_key = "date"
    },
  ]
  buckets = [
  ]
  env_files = var.env_files
  domain_base = var.domain_base
}
