# Package the windowing handler. boto3 is already in the Lambda runtime, so the
# zip is just the source file.
data "archive_file" "windowing" {
  type        = "zip"
  source_file = "${local.app_dir}/windowing/handler.py"
  output_path = "${path.module}/.build/windowing.zip"
}

resource "aws_cloudwatch_log_group" "windowing" {
  name              = "/aws/lambda/${local.windowing_fn}"
  retention_in_days = 3
}

# The windowing application: reads the stream in one-minute tumbling windows,
# computes count and average per category, dedups by event_id, sets invalid
# events aside and writes the per-window results to DynamoDB.
resource "aws_lambda_function" "windowing" {
  function_name    = local.windowing_fn
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.windowing.output_path
  source_code_hash = data.archive_file.windowing.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      RESULTS_TABLE  = aws_dynamodb_table.ventanas.name
      DEDUP_TABLE    = aws_dynamodb_table.vistos.name
      REJECTS_TABLE  = aws_dynamodb_table.invalidos.name
      COUNTERS_TABLE = aws_dynamodb_table.contadores.name
      AMOUNT_MIN     = tostring(var.amount_min)
      AMOUNT_MAX     = tostring(var.amount_max)
    }
  }

  depends_on = [aws_cloudwatch_log_group.windowing]
}

# Connect the Lambda to the stream with a tumbling window, so state (the running
# per-category aggregate) is carried across invocations within each window.
resource "aws_lambda_event_source_mapping" "windowing" {
  event_source_arn                   = aws_kinesis_stream.transacciones.arn
  function_name                      = aws_lambda_function.windowing.arn
  starting_position                  = "LATEST"
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = 10
  tumbling_window_in_seconds         = var.window_seconds

  # One failing batch must not stall the demo stream.
  maximum_retry_attempts         = 2
  bisect_batch_on_function_error = true
}
