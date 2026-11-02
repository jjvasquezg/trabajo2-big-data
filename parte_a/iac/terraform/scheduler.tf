resource "aws_scheduler_schedule" "pipeline" {
  name                         = "${var.project}-pipeline-diario"
  description                  = "Inicia la maquina de estados del trabajo 2, parte A"
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone
  state                        = var.schedule_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn

    # Queda en la entrada de la ejecucion: prueba que la inicio el horario
    input = jsonencode({
      origen  = "EventBridge Scheduler"
      horario = "${var.project}-pipeline-diario"
    })
  }
}
