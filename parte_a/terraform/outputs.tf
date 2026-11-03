output "bucket" {
  description = "Bucket del lago de datos"
  value       = aws_s3_bucket.lake.id
}

output "state_machine_arn" {
  description = "ARN de la maquina de estados"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "schedule_name" {
  description = "Nombre del horario de EventBridge Scheduler"
  value       = aws_scheduler_schedule.pipeline.name
}

output "subir_el_csv" {
  description = "Comando para subir el CSV de Santander"
  value       = "aws s3 cp train_ver2.csv s3://${aws_s3_bucket.lake.id}/raw/santander/train_ver2.csv"
}

output "iniciar_la_maquina_de_estados" {
  description = "Comando para iniciar una ejecucion a mano"
  value       = "aws stepfunctions start-execution --state-machine-arn ${aws_sfn_state_machine.pipeline.arn}"
}
