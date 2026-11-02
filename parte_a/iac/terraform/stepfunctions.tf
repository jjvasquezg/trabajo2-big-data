resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project}-pipeline-batch"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"

  # La misma definicion que se pega en la consola (modo Code)
  definition = file("${local.parte_a}/stepfunctions/state_machine.asl.json")
}
