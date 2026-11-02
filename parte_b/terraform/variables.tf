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
  description = "Nombre del bucket de eventos crudos. Si es vacio, se usa <project>-parte-b-<id de la cuenta>"
  type        = string
  default     = ""
}

variable "force_destroy_bucket" {
  description = "Permite que terraform destroy borre el bucket aunque tenga objetos"
  type        = bool
  default     = true
}

variable "shard_count" {
  description = "Numero de shards del stream de Kinesis (1 basta para la demo)"
  type        = number
  default     = 1
}

variable "stream_retention_hours" {
  description = "Horas de retencion de los registros en el stream (minimo 24)"
  type        = number
  default     = 24
}

variable "window_seconds" {
  description = "Tamano de la ventana de tiempo en segundos (el enunciado pide 1 minuto)"
  type        = number
  default     = 60
}

variable "batch_size" {
  description = "Maximo de registros por invocacion de la Lambda de ventanas"
  type        = number
  default     = 200
}

variable "amount_min" {
  description = "Limite inferior de un monto valido"
  type        = number
  default     = 0
}

variable "amount_max" {
  description = "Limite superior de un monto valido; por fuera el evento es invalido"
  type        = number
  default     = 50000
}

variable "firehose_buffer_seconds" {
  description = "Segundos que Firehose acumula antes de escribir a S3 (60 a 900)"
  type        = number
  default     = 60
}

variable "firehose_buffer_mb" {
  description = "MB que Firehose acumula antes de escribir a S3 (1 a 128)"
  type        = number
  default     = 1
}
