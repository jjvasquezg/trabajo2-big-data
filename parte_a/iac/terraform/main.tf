data "aws_caller_identity" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project}-trabajo2-${local.account_id}"

  # Los scripts y la dimension de provincias viven en parte-a/, fuera de iac/,
  # porque son los mismos archivos que se suben a mano en la guia de la consola
  parte_a = "${path.module}/../../parte-a"

  # Los mismos nombres que la maquina de estados (state_machine.asl.json)
  job_limpieza    = "${var.project}-limpieza-join"
  job_altas       = "${var.project}-rama-altas-producto-mes"
  job_penetracion = "${var.project}-rama-penetracion-provincia"

  glue_argumentos = {
    "--BUCKET"              = aws_s3_bucket.lake.id
    "--job-language"        = "python"
    "--enable-metrics"      = "true"
    "--enable-job-insights" = "false"
    "--enable-spark-ui"     = "false"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--TempDir"             = "s3://${aws_s3_bucket.lake.id}/tmp/"
  }

  columnas_santander = [
    "fecha_dato", "ncodpers", "ind_empleado", "pais_residencia", "sexo", "age",
    "fecha_alta", "ind_nuevo", "antiguedad", "indrel", "ult_fec_cli_1t",
    "indrel_1mes", "tiprel_1mes", "indresi", "indext", "conyuemp",
    "canal_entrada", "indfall", "tipodom", "cod_prov", "nomprov",
    "ind_actividad_cliente", "renta", "segmento",
    "ind_ahor_fin_ult1", "ind_aval_fin_ult1", "ind_cco_fin_ult1",
    "ind_cder_fin_ult1", "ind_cno_fin_ult1", "ind_ctju_fin_ult1",
    "ind_ctma_fin_ult1", "ind_ctop_fin_ult1", "ind_ctpp_fin_ult1",
    "ind_deco_fin_ult1", "ind_deme_fin_ult1", "ind_dela_fin_ult1",
    "ind_ecue_fin_ult1", "ind_fond_fin_ult1", "ind_hip_fin_ult1",
    "ind_plan_fin_ult1", "ind_pres_fin_ult1", "ind_reca_fin_ult1",
    "ind_tjcr_fin_ult1", "ind_valo_fin_ult1", "ind_viv_fin_ult1",
    "ind_nomina_ult1", "ind_nom_pens_ult1", "ind_recibo_ult1",
  ]

  productos = [for c in local.columnas_santander : c if startswith(c, "ind_") && endswith(c, "ult1")]

  # Columnas de la capa silver, en el orden en que las escribe el trabajo inicial
  columnas_silver = concat(
    [
      { name = "ncodpers", type = "bigint" },
      { name = "ind_empleado", type = "string" },
      { name = "pais_residencia", type = "string" },
      { name = "sexo", type = "string" },
      { name = "age", type = "int" },
      { name = "fecha_alta", type = "date" },
      { name = "ind_nuevo", type = "int" },
      { name = "antiguedad", type = "int" },
      { name = "indrel", type = "int" },
      { name = "indrel_1mes", type = "string" },
      { name = "tiprel_1mes", type = "string" },
      { name = "indresi", type = "string" },
      { name = "indext", type = "string" },
      { name = "canal_entrada", type = "string" },
      { name = "indfall", type = "string" },
      { name = "cod_prov", type = "int" },
      { name = "ind_actividad_cliente", type = "int" },
      { name = "renta", type = "double" },
      { name = "segmento", type = "string" },
    ],
    [for p in local.productos : { name = p, type = "int" }],
    [
      { name = "edad_imputada", type = "boolean" },
      { name = "renta_imputada", type = "boolean" },
      { name = "provincia", type = "string" },
      { name = "comunidad_autonoma", type = "string" },
      { name = "poblacion_2016", type = "bigint" },
      { name = "renta_bruta_hogar_2015", type = "int" },
      { name = "renta_neta_persona_2015", type = "int" },
      { name = "num_productos", type = "int" },
    ],
  )

  columnas_altas = [
    { name = "fecha_dato", type = "date" },
    { name = "producto", type = "string" },
    { name = "nombre_producto", type = "string" },
    { name = "clientes_con_producto", type = "bigint" },
    { name = "altas", type = "bigint" },
    { name = "bajas", type = "bigint" },
  ]

  columnas_penetracion = [
    { name = "fecha_dato", type = "date" },
    { name = "cod_prov", type = "int" },
    { name = "provincia", type = "string" },
    { name = "comunidad_autonoma", type = "string" },
    { name = "poblacion_2016", type = "bigint" },
    { name = "renta_neta_persona_2015", type = "int" },
    { name = "clientes", type = "bigint" },
    { name = "productos_promedio", type = "double" },
    { name = "pct_nomina", type = "double" },
    { name = "pct_tarjeta_credito", type = "double" },
    { name = "clientes_por_mil_hab", type = "double" },
    { name = "brecha_clientes", type = "bigint" },
  ]
}
