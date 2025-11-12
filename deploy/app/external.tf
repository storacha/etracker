locals {
    storage_provider_table_name = "${terraform.workspace == "prod" ? "prod-upload-api-storage-provider" : "staging-warm-upload-api-storage-provider"}"
    storage_provider_table_region = "${terraform.workspace == "prod" ? "us-west-2" : "us-east-2"}"

    customer_table_name = "${terraform.workspace == "prod" ? "prod-upload-api-customer" : "staging-warm-upload-api-customer"}"
    customer_table_region = "${terraform.workspace == "prod" ? "us-west-2" : "us-east-2"}"

    consumer_table_name = "${terraform.workspace == "prod" ? "prod-upload-api-consumer" : "staging-warm-upload-api-consumer"}"
    consumer_table_region = "${terraform.workspace == "prod" ? "us-west-2" : "us-east-2"}"
}

provider "aws" {
  alias = "storage_provider"
  region = local.storage_provider_table_region
}

data "aws_dynamodb_table" "storage_provider_table" {
  provider = aws.storage_provider
  name = local.storage_provider_table_name
}

provider "aws" {
  alias = "customer"
  region = local.customer_table_region
}

data "aws_dynamodb_table" "customer_table" {
  provider = aws.customer
  name = local.customer_table_name
}

provider "aws" {
  alias = "consumer"
  region = local.consumer_table_region
}

data "aws_dynamodb_table" "consumer_table" {
  provider = aws.consumer
  name = local.consumer_table_name
}

data "aws_iam_policy_document" "task_external_dynamodb_scan_query_document" {
  statement {
    actions = [
      "dynamodb:Scan",
      "dynamodb:Query",
    ]
    resources = [
      data.aws_dynamodb_table.storage_provider_table.arn,
      data.aws_dynamodb_table.customer_table.arn,
      data.aws_dynamodb_table.consumer_table.arn,
      "${data.aws_dynamodb_table.consumer_table.arn}/index/consumer",
      "${data.aws_dynamodb_table.consumer_table.arn}/index/customer",
    ]
  }
}

resource "aws_iam_policy" "task_external_dynamodb_scan_query" {
  name        = "${terraform.workspace}-${var.app}-task-external-dynamodb-scan-query"
  description = "This policy will be used by the ECS task to scan and query data from external DynamoDB tables"
  policy      = data.aws_iam_policy_document.task_external_dynamodb_scan_query_document.json
}

resource "aws_iam_role_policy_attachment" "task_external_dynamodb_scan_query" {
  role       = module.app.deployment.task_role.name
  policy_arn = aws_iam_policy.task_external_dynamodb_scan_query.arn
}
