# Trabajo 1: capas Medallion y streaming

**Curso:** Almacenamiento y Procesamiento de Grandes Datos (SI7006/SI6003), Universidad EAFIT, 2026.
**Enunciado:** [APGD-trabajo1-262.pdf](APGD-trabajo1-262.pdf)

| Entrega | Información |
| --- | --- |
| Integrante 1 | Juan Felipe Restrepo |
| Integrante 2 | Manuela Castaño |
| Integrante 3 | Juan Jose Vasquez |

## Video explicativo: [Ver en YouTube](https://youtu.be/7iNTRJhWV-o)

## ¿Qué contiene el proyecto?

El trabajo aplica dos formas de procesar datos en AWS:

| Parte | Propósito | Recorrido | Guía detallada |
| --- | --- | --- | --- |
| **A: procesamiento por lotes** | Preparar datos de clientes y productos bancarios para analizar nuevas contrataciones mensuales. | CSV en S3 → tabla externa de Athena (**bronce**) → validación y tablas Iceberg (**plata**) → agregación Iceberg (**oro**). | [README de la parte A](parteA/README.md) |
| **B: streaming** | Recibir eventos de recomendación, validarlos, almacenarlos y medir su latencia. | IoT Core → SQS → Lambda → DynamoDB. | [README de la parte B](parteB/README.md) |

Los recursos de AWS se crean manualmente en la consola, como pide el [enunciado](APGD-trabajo1-262.pdf). El repositorio conserva las consultas SQL y los scripts Python, pero no incluye el conjunto de datos ni las credenciales de AWS.

## Parte A: de datos crudos a una respuesta de negocio

La fuente es el conjunto de datos [Santander Product Recommendation en Kaggle](https://www.kaggle.com/c/santander-product-recommendation): un CSV de observaciones mensuales de clientes, con identificadores, fechas, atributos del cliente y 24 indicadores de tenencia de productos. El archivo se ubica en `s3://bigdata-trabajo1/bronze/santander/`; si se usa otra ruta, hay que actualizar las ubicaciones S3 de los scripts. El número real de filas y el tamaño del archivo se comprueban en el entorno de AWS; el repositorio no contiene esos resultados.

1. **Bronce:** [`parteA/02_bronce_crear_tabla_externa.sql`](parteA/02_bronce_crear_tabla_externa.sql) crea `santander.bronze` sobre el CSV. Todas las columnas se leen como texto para conservar los valores originales. Las consultas `03` a `07` revisan cantidad de filas, muestras, distribución mensual y formatos.
2. **Hallazgos:** [`parteA/08_hallazgos_tabla_calidad.sql`](parteA/08_hallazgos_tabla_calidad.sql) cuantifica duplicados, vacíos, fechas y números inválidos, entre otros defectos. Incluye la acción prevista para cada hallazgo.
3. **Plata:** la vista [`parteA/09_plata_vista_reglas_calidad.sql`](parteA/09_plata_vista_reglas_calidad.sql) asigna a cada fila el primer motivo de rechazo que corresponda. Aplica seis reglas: llave válida, unicidad por cliente y fecha, perfil sociodemográfico presente, edad válida, indicadores de producto binarios y antigüedad válida. Después se crean `santander.silver` con datos tipados y limpios y `santander.silver_rechazadas` con los valores originales y el motivo. [`parteA/12_plata_cuadre_conteos.sql`](parteA/12_plata_cuadre_conteos.sql) verifica **bronce = válidas + rechazadas**.
4. **Oro:** [`parteA/14_oro_tabla_contrataciones_iceberg.sql`](parteA/14_oro_tabla_contrataciones_iceberg.sql) calcula, por mes, segmento y producto, cuántos clientes pasaron de no tenerlo el mes anterior a tenerlo en el mes actual. También calcula una tasa de conversión sobre los clientes que no lo tenían. [`parteA/15_oro_top7_mayo_2016.sql`](parteA/15_oro_top7_mayo_2016.sql) consulta los siete productos con más contrataciones nuevas en mayo de 2016.
5. **Comparación:** las consultas `16`, `17` y `18` responden la misma pregunta sobre bronce, plata y oro. En Athena se comparan los datos escaneados y el tiempo de ejecución; los valores medidos no están guardados en este repositorio.

Las tablas plata y oro se materializan en **Iceberg/Parquet**. Para reproducir el proceso en orden y conocer las dependencias de cada consulta, consulta la [guía de la parte A](parteA/README.md).

### Pregunta final: ¿qué cambiaríamos si los datos fueran mil veces mayores?

Serían aproximadamente **2,1 TB y 13.600 millones de filas**. Haríamos cinco cambios:

1. Convertir el CSV de bronce a **Parquet comprimido** desde la ingesta para reducir lecturas y costos.
2. Construir plata y oro con **Spark en AWS Glue o EMR**, en lugar de procesar todo con Athena.
3. **Particionar por mes** y agrupar los datos por cliente para acelerar las consultas y comparaciones mensuales.
4. Incorporar solo el mes nuevo mediante **`MERGE` de Iceberg**, sin reconstruir las tablas completas.
5. **Compactar archivos pequeños con `OPTIMIZE`** para mantener eficiente la lectura.

## Parte B: eventos en tiempo real

[`parteB/publish_events.py`](parteB/publish_events.py) publica **10.000 eventos** en el tema `finsir/events` de IoT Core. Una regla añade `iot_received_ms` y envía cada mensaje a SQS. [`parteB/lambda.py`](parteB/lambda.py) consume la cola, comprueba el esquema y guarda los eventos válidos en la tabla `finsir-events` de DynamoDB. La clave `event_id` permite evitar sobrescrituras cuando SQS entrega un mensaje más de una vez. Los eventos inválidos se registran como rechazados en los logs de Lambda; los errores técnicos se solicitan para reintento.

El productor omite `product_id` en el **1 %** de los eventos para probar el rechazo. El [README de la parte B](parteB/README.md) informa para la ejecución documentada **10.000 publicados = 9.900 guardados + 100 descartados**. También informa una latencia promedio de **73,53 ms** y máxima de **3.320,77 ms**, calculadas desde la recepción en IoT Core hasta el instante de procesamiento en Lambda. [`parteB/measure_latency.py`](parteB/measure_latency.py) lee las latencias almacenadas en DynamoDB para resumirlas.

La [guía de la parte B](parteB/README.md) explica el esquema del evento, la configuración manual de los servicios, los comandos de ejecución y el motivo por el que se usa la marca de tiempo de IoT Core para medir la latencia.

### Pregunta final: ¿cuándo cambiaríamos esta arquitectura de streaming?

La ejecución documentada procesó **10.000 eventos** con una latencia promedio de **73,53 ms**; para ese volumen mantendríamos el flujo actual por su sencillez. Evaluaríamos **Amazon Kinesis Data Streams** si llegaran miles de eventos por segundo de forma sostenida, la cola acumulara mensajes o la latencia dejara de cumplir los requisitos. Kinesis facilitaría escalar el flujo y permitiría varios consumidores. No hay un umbral único: la decisión depende del ritmo de llegada, la latencia requerida y los costos.

## Estructura del repositorio

```text
.
├── APGD-trabajo1-262.pdf   # enunciado
├── README.md               # visión general de ambas partes
├── parteA/
│   ├── README.md           # ejecución y explicación de las consultas
│   └── 01_...19_*.sql      # flujo Medallion y consultas de comprobación
└── parteB/
    ├── README.md           # configuración, ejecución y resultados del streaming
    ├── publish_events.py   # publicación de eventos en IoT Core
    ├── lambda.py           # validación y persistencia en DynamoDB
    └── measure_latency.py  # resumen de latencias
```
