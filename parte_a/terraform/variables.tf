variable "region" {
  description = "Region de AWS donde se crea todo"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Prefijo de los nombres de los recursos"
  type        = string
  default     = "finsir"
}

variable "bucket_name" {
  description = "Nombre del bucket. Si es vacio, se usa <project>-trabajo2-<id de la cuenta>"
  type        = string
  default     = ""
}

variable "force_destroy_bucket" {
  description = "Permite que terraform destroy borre el bucket aunque tenga objetos"
  type        = bool
  default     = true
}

variable "workers_limpieza" {
  description = "Workers G.1X del trabajo de Glue inicial (minimo 2)"
  type        = number
  default     = 5
}

variable "workers_ramas" {
  description = "Workers G.1X de cada trabajo de Glue de las ramas (minimo 2)"
  type        = number
  default     = 4
}

variable "schedule_expression" {
  description = "Horario de EventBridge Scheduler: cron(...), rate(...) o at(yyyy-mm-ddThh:mm:ss) para una sola vez"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "schedule_timezone" {
  description = "Zona horaria con que se interpreta schedule_expression"
  type        = string
  default     = "America/Bogota"
}

variable "schedule_enabled" {
  description = "Un horario activo ejecuta el pipeline completo (cuesta unos centavos de Glue por ejecucion)"
  type        = bool
  default     = false
}
