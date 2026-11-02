data "aws_caller_identity" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project}-parte-b-${local.account_id}"

  # The producer, the windowing Lambda and the query script live in app/,
  # outside terraform/, because they are the same files the team runs by hand.
  app_dir = "${path.module}/../app"

  # Resource names. Keep them in sync with query_results.py, which builds the
  # DynamoDB table names from <project> and these suffixes.
  stream_name   = "${var.project}-transacciones"
  windowing_fn  = "${var.project}-ventanas"
  firehose_name = "${var.project}-eventos-crudos"

  results_table  = "${var.project}-ventanas"   # per-window per-category aggregates
  dedup_table    = "${var.project}-vistos"     # seen event_ids (dedup guard)
  rejects_table  = "${var.project}-invalidos"  # invalid events with their reason
  counters_table = "${var.project}-contadores" # reconciliation counters
}
