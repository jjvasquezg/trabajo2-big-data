output "stream_name" {
  description = "Nombre del stream de Kinesis"
  value       = aws_kinesis_stream.transacciones.name
}

output "bucket" {
  description = "Bucket con los eventos crudos"
  value       = aws_s3_bucket.crudos.id
}

output "windowing_function" {
  description = "Lambda que calcula las ventanas"
  value       = aws_lambda_function.windowing.function_name
}

output "firehose" {
  description = "Firehose que guarda los eventos crudos en S3"
  value       = aws_kinesis_firehose_delivery_stream.crudos.name
}

output "results_table" {
  description = "Tabla de DynamoDB con el resultado de cada ventana"
  value       = aws_dynamodb_table.ventanas.name
}

output "rejects_table" {
  description = "Tabla de DynamoDB con los eventos invalidos y su motivo"
  value       = aws_dynamodb_table.invalidos.name
}

output "counters_table" {
  description = "Tabla de DynamoDB con los contadores de reconciliacion"
  value       = aws_dynamodb_table.contadores.name
}

output "correr_el_productor" {
  description = "Comando para publicar eventos durante 5 minutos"
  value       = "python app/producer.py --stream ${aws_kinesis_stream.transacciones.name} --minutes 5 --region ${var.region}"
}

output "consultar_resultados" {
  description = "Comando para consultar DynamoDB y verificar que los conteos cuadran"
  value       = "python app/query_results.py --prefix ${var.project} --region ${var.region}"
}
