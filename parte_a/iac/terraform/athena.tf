resource "aws_glue_catalog_database" "finsir" {
  name = var.project
}

resource "aws_athena_workgroup" "finsir" {
  name          = var.project
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.id}/athena-results/"
    }
  }
}

# Capa bronze: el CSV tal como llego, todo texto
resource "aws_glue_catalog_table" "bronze_santander" {
  name          = "bronze_santander"
  database_name = aws_glue_catalog_database.finsir.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                 = "TRUE"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.id}/raw/santander/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
      parameters = {
        separatorChar = ","
        quoteChar     = "\""
      }
    }

    dynamic "columns" {
      for_each = local.columnas_santander
      content {
        name = columns.value
        type = "string"
      }
    }
  }
}

resource "aws_glue_catalog_table" "bronze_dim_provincia" {
  name          = "bronze_dim_provincia"
  database_name = aws_glue_catalog_database.finsir.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                 = "TRUE"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.id}/raw/dim_provincia/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
      parameters = {
        separatorChar = ","
        quoteChar     = "\""
      }
    }

    columns {
      name = "cod_prov"
      type = "string"
    }
    columns {
      name = "provincia"
      type = "string"
    }
    columns {
      name = "comunidad_autonoma"
      type = "string"
    }
    columns {
      name = "poblacion_2016"
      type = "string"
    }
    columns {
      name = "renta_bruta_hogar_2015"
      type = "string"
    }
    columns {
      name = "renta_neta_persona_2015"
      type = "string"
    }
  }
}

# Capa silver: Parquet particionado por mes. Con partition projection la tabla
# encuentra las carpetas sola, sin MSCK REPAIR TABLE. Los cortes caen siempre
# el dia 28, asi que un intervalo de un mes los recorre todos.
resource "aws_glue_catalog_table" "silver_clientes" {
  name          = "silver_clientes"
  database_name = aws_glue_catalog_database.finsir.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                              = "TRUE"
    "parquet.compression"                 = "SNAPPY"
    "projection.enabled"                  = "true"
    "projection.fecha_dato.type"          = "date"
    "projection.fecha_dato.format"        = "yyyy-MM-dd"
    "projection.fecha_dato.range"         = "2015-01-28,NOW"
    "projection.fecha_dato.interval"      = "1"
    "projection.fecha_dato.interval.unit" = "MONTHS"
    "storage.location.template"           = "s3://${aws_s3_bucket.lake.id}/silver/clientes/fecha_dato=$${fecha_dato}/"
  }

  partition_keys {
    name = "fecha_dato"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.id}/silver/clientes/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.columnas_silver
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# Capa gold: las dos agrupaciones de las ramas
resource "aws_glue_catalog_table" "gold_altas_producto_mes" {
  name          = "gold_altas_producto_mes"
  database_name = aws_glue_catalog_database.finsir.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.id}/gold/altas_producto_mes/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.columnas_altas
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "gold_penetracion_provincia" {
  name          = "gold_penetracion_provincia"
  database_name = aws_glue_catalog_database.finsir.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.id}/gold/penetracion_provincia/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.columnas_penetracion
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
