locals {
    external_storage_provider_table_name = "${terraform.workspace == "prod" ? "prod-upload-api-storage-provider" : "staging-warm-upload-api-storage-provider"}"
    external_storage_provider_table_region = "${terraform.workspace == "prod" ? "us-west-2" : "us-east-2"}"
}

provider "aws" {
  alias = "external_storage_provider"
  region = local.external_storage_provider_table_region
}

data "aws_dynamodb_table" "external_storage_provider_table" {
  provider = aws.external_storage_provider
  name = local.external_storage_provider_table_name
}

data "aws_iam_policy_document" "task_external_dynamodb_scan_query_document" {
  statement {
    actions = [
      "dynamodb:Scan",
      "dynamodb:Query",
    ]
    resources = [
      data.aws_dynamodb_table.external_storage_provider_table.arn,
      "${data.aws_dynamodb_table.external_storage_provider_table.arn}/index/*",
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
