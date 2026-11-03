# Declaración de uso de IA

Este documento detalla qué artefactos del repositorio se crearon o mejoraron con apoyo de herramientas de inteligencia artificial. El equipo revisó todos estos materiales, validó que correspondan a la implementación realizada y puede explicar su funcionamiento.

## Código Python

Se usó IA para crear todos los archivos de código Python del repositorio:

- Los scripts de ETL de AWS Glue de la parte A (limpieza y join, rama de altas por producto y mes, y rama de penetración por provincia).
- La función Lambda de la parte B, que calcula conteos y promedios por categoría en ventanas de tiempo, descarta duplicados e inválidos y escribe en DynamoDB.
- El código de ejecución del flujo de la parte B (el productor que publica los eventos y cualquier script de orquestación o prueba asociado).

## Infraestructura, SQL y políticas

Se usó IA para crear:

- Toda la infraestructura como código en Terraform (archivos `.tf`) de ambas partes: buckets, roles, trabajos de Glue, máquina de estados, horarios, streams, Lambda, Firehose, tablas y demás recursos.
- Todas las sentencias y archivos SQL (creación de tablas y consultas de Athena).
- Todas las políticas de IAM y los documentos de permisos en formato JSON.

## Documentación

Se usó IA para complementar y mejorar los archivos de documentación Markdown (`.md`) del repositorio, incluidos este documento y los README de la raíz, la parte A y la parte B.
