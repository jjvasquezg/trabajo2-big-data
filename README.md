# Trabajo 2: procesamiento de datos en AWS

**Curso:** Almacenamiento de Datos (SI7006), Universidad EAFIT, 2026-2.

**Enunciado:** [SI7006-262-trabajo2.pdf](SI7006-262-trabajo2.pdf)

| Entrega | Información |
| --- | --- |
| Integrante 1 | Juan Felipe Restrepo |
| Integrante 2 | Manuela Castaño |
| Integrante 3 | Juan Jose Vasquez |

## Video explicativo: [Ver en YouTube](https://www.youtube.com/watch?v=_40U-gqOp2A)

## ¿Qué contiene el proyecto?

Este repositorio reúne las dos partes del trabajo práctico de Big Data del proyecto FINSIR. La parte A implementa un flujo batch y la parte B un flujo de streaming. En sus respectivos README se encuentra el paso a paso para crear y replicar cada solución, junto con el contexto y los recursos utilizados.

| Parte | Propósito | Flujo general | Guía detallada |
| --- | --- | --- | --- |
| **A: pipeline batch** | Procesar y analizar datos de clientes y productos bancarios de Santander, enriquecidos con información provincial del INE. | Datos CSV en S3 → AWS Glue limpia, transforma y combina → S3 guarda Parquet → Step Functions ejecuta dos ramas de análisis → Athena consulta los resultados. | [README de la parte A](parte_a/README.md) |
| **B: streaming** | Procesar eventos financieros en tiempo real, calcular métricas por categoría y conservar los eventos originales. | Productor → Kinesis Data Streams → Lambda y Firehose → DynamoDB y S3. | [README de la parte B](parte_b/README.md) |

## Parte A: pipeline batch

La parte A procesa el conjunto de datos de recomendaciones de productos de Santander junto con una dimensión de provincias del INE. AWS Glue limpia y transforma los datos, los combina y los guarda en S3 como Parquet. AWS Step Functions coordina el trabajo inicial y dos ramas de análisis en paralelo; EventBridge Scheduler puede iniciar el flujo y Athena permite consultar los resultados.

La guía de [parte A](parte_a/README.md) explica el proceso manual desde la consola de AWS y su despliegue automatizado con Terraform, además de los datos, las consultas y la eliminación de recursos.

## Parte B: streaming

La parte B publica eventos financieros en Amazon Kinesis Data Streams. Una función Lambda calcula conteos y promedios por categoría en ventanas de un minuto, descarta duplicados e inválidos y guarda los resultados en DynamoDB. Amazon Data Firehose conserva en S3 una copia de los eventos crudos.

La guía de [parte B](parte_b/README.md) describe la arquitectura y los pasos para crear los recursos manualmente desde la consola o desplegarlos con Terraform, ejecutar el productor y consultar los resultados.

## Declaración de uso de IA

El detalle de qué artefactos del repositorio se apoyaron en herramientas de inteligencia artificial está en [declaracion_uso_ia.md](declaracion_uso_ia.md).
