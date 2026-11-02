# The stream that the producer writes to and that both consumers read from:
# the windowing Lambda and the Firehose raw consumer.
resource "aws_kinesis_stream" "transacciones" {
  name             = local.stream_name
  shard_count      = var.shard_count
  retention_period = var.stream_retention_hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}
