resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy_bucket
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Los scripts de Glue
resource "aws_s3_object" "script_limpieza" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/01_limpieza_join.py"
  source = "${local.parte_a}/glue/01_limpieza_join.py"
  etag   = filemd5("${local.parte_a}/glue/01_limpieza_join.py")
}

resource "aws_s3_object" "script_altas" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/02_rama_altas_producto_mes.py"
  source = "${local.parte_a}/glue/02_rama_altas_producto_mes.py"
  etag   = filemd5("${local.parte_a}/glue/02_rama_altas_producto_mes.py")
}

resource "aws_s3_object" "script_penetracion" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/03_rama_penetracion_provincia.py"
  source = "${local.parte_a}/glue/03_rama_penetracion_provincia.py"
  etag   = filemd5("${local.parte_a}/glue/03_rama_penetracion_provincia.py")
}

# El segundo conjunto de datos, el que se une por cod_prov
resource "aws_s3_object" "dim_provincia" {
  bucket = aws_s3_bucket.lake.id
  key    = "raw/dim_provincia/dim_provincia.csv"
  source = "${local.parte_a}/data/dim_provincia.csv"
  etag   = filemd5("${local.parte_a}/data/dim_provincia.csv")
}

# El CSV de Santander (2.3 GB) no se sube con terraform: usa
#   aws s3 cp train_ver2.csv s3://<bucket>/raw/santander/train_ver2.csv
