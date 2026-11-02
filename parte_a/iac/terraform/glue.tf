resource "aws_glue_job" "limpieza_join" {
  name              = local.job_limpieza
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "6.0"
  worker_type       = "G.1X"
  number_of_workers = var.workers_limpieza
  timeout           = 60
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.id}/${aws_s3_object.script_limpieza.key}"
  }

  default_arguments = local.glue_argumentos
}

resource "aws_glue_job" "rama_altas" {
  name              = local.job_altas
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "6.0"
  worker_type       = "G.1X"
  number_of_workers = var.workers_ramas
  timeout           = 60
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.id}/${aws_s3_object.script_altas.key}"
  }

  default_arguments = local.glue_argumentos
}

resource "aws_glue_job" "rama_penetracion" {
  name              = local.job_penetracion
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "6.0"
  worker_type       = "G.1X"
  number_of_workers = var.workers_ramas
  timeout           = 60
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.id}/${aws_s3_object.script_penetracion.key}"
  }

  default_arguments = local.glue_argumentos
}
