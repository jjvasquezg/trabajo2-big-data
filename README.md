# Trabajo 2: procesamiento de datos en AWS

Este repositorio contiene las dos partes del trabajo práctico de Big Data del proyecto FINSIR. La parte A implementa un flujo batch y la parte B un flujo de streaming. En sus respectivos README se encuentran el contexto, los recursos utilizados y el paso a paso para crear y replicar cada solución.

## Parte A: pipeline batch

La parte A procesa el conjunto de datos de recomendaciones de productos de Santander junto con una dimensión de provincias del INE. AWS Glue limpia y transforma los datos, los combina y los guarda en S3 como Parquet. AWS Step Functions coordina el trabajo inicial y dos ramas de análisis en paralelo; EventBridge Scheduler puede iniciar el flujo y Athena permite consultar los resultados.

La guía de [parte A](parte_a/README.md) explica el proceso manual desde la consola de AWS y su despliegue automatizado con Terraform, además de los datos, las consultas y la eliminación de recursos.

## Parte B: streaming

La parte B publica eventos financieros en Amazon Kinesis Data Streams. Una función Lambda calcula conteos y promedios por categoría en ventanas de un minuto, descarta duplicados e inválidos y guarda los resultados en DynamoDB. Amazon Data Firehose conserva en S3 una copia de los eventos crudos.

La guía de [parte B](parte_b/README.md) describe la arquitectura y los pasos para crear los recursos manualmente desde la consola o desplegarlos con Terraform, ejecutar el productor y consultar los resultados.

## Declaración de uso de IA

Se utilizó una herramienta de inteligencia artificial como apoyo para crear y revisar scripts, documentación y sentencias o artefactos de Terraform incluidos en el repositorio. El equipo debe revisar esos materiales, validar que correspondan a la implementación realizada y poder explicar su funcionamiento.
