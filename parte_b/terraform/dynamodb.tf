# Result of each window: one item per (window_start, category), with the event
# count and the average amount. This is the table the project question queries.
resource "aws_dynamodb_table" "ventanas" {
  name         = local.results_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "window_start"
  range_key    = "category"

  attribute {
    name = "window_start"
    type = "S"
  }

  attribute {
    name = "category"
    type = "S"
  }
}

# Seen event_ids. A conditional write here is the atomic dedup guard, so a
# repeated event is counted once even across Lambda invocations. TTL keeps it
# small after the demo.
resource "aws_dynamodb_table" "vistos" {
  name         = local.dedup_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"

  attribute {
    name = "event_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Invalid events set aside, each with the reason it was discarded.
resource "aws_dynamodb_table" "invalidos" {
  name         = local.rejects_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"

  attribute {
    name = "event_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Global reconciliation counters: unique_processed, duplicates_discarded,
# invalid_discarded. Their sum must equal the number of events the producer
# published.
resource "aws_dynamodb_table" "contadores" {
  name         = local.counters_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "counter"

  attribute {
    name = "counter"
    type = "S"
  }
}
