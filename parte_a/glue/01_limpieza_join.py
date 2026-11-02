import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

COLUMNAS = [
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
PRODUCTOS = [c for c in COLUMNAS if c.startswith("ind_") and c.endswith("ult1")]
COLUMNAS_INT = [
    "age", "ind_nuevo", "antiguedad", "indrel", "cod_prov", "ind_actividad_cliente",
]
# Columnas de texto cuyo vacio se reemplaza por una etiqueta en vez de un null
COLUMNAS_DESCONOCIDO = [
    "ind_empleado", "pais_residencia", "sexo", "indrel_1mes", "tiprel_1mes",
    "indresi", "indext", "canal_entrada", "indfall", "segmento",
]
# Columnas que se descartan: vacias casi en su totalidad, constantes, o
# reemplazadas por la dimension de provincias
COLUMNAS_DESCARTADAS = ["ult_fec_cli_1t", "conyuemp", "tipodom", "nomprov"]
# Lo que aporta dim_provincia.csv al unirlo por cod_prov
COLUMNAS_PROVINCIA = [
    "provincia", "comunidad_autonoma", "poblacion_2016",
    "renta_bruta_hogar_2015", "renta_neta_persona_2015",
]
COLUMNAS_SILVER = (
    [c for c in COLUMNAS if c not in COLUMNAS_DESCARTADAS]
    + ["edad_imputada", "renta_imputada"]
    + COLUMNAS_PROVINCIA
    + ["num_productos"]
)
EDAD_MINIMA, EDAD_MAXIMA = 0, 122  # 122 anos: la edad documentada mas alta


def leer_csv(spark, ruta):
    """Lee el CSV con el esquema explicito, todo como texto: evita una pasada
    extra de inferSchema sobre 2.3 GB y deja la limpieza en nuestras manos."""
    esquema = StructType([StructField(c, StringType()) for c in COLUMNAS])
    return spark.read.csv(ruta, header=True, schema=esquema, quote='"', escape='"')


def normalizar(df):
    """Quita los espacios, convierte '' y 'NA' en null y asigna los tipos."""
    texto = {}
    for c in df.columns:
        valor = F.trim(F.col(c))
        texto[c] = F.when((valor == "") | (valor == "NA"), None).otherwise(valor)
    df = df.select([texto[c].alias(c) for c in df.columns])

    tipado = {
        "fecha_dato": F.to_date("fecha_dato", "yyyy-MM-dd"),
        "fecha_alta": F.to_date("fecha_alta", "yyyy-MM-dd"),
        "ncodpers": F.col("ncodpers").cast("long"),
        "renta": F.col("renta").cast("double"),
        # '1.0' y '1' son el mismo tipo de cliente
        "indrel_1mes": F.regexp_replace("indrel_1mes", r"\.0$", ""),
    }
    for c in COLUMNAS_INT:
        tipado[c] = F.col(c).cast("int")
    # Un producto sin dato (NA en nomina y pension) es un producto que no tiene
    for c in PRODUCTOS:
        tipado[c] = F.coalesce(F.col(c).cast("int"), F.lit(0))
    return df.select([tipado.get(c, F.col(c)).alias(c) for c in df.columns])


def es_valido():
    """Un registro es valido si se sabe de quien y de cuando es, y desde cuando
    es cliente. Las 27734 filas sin fecha_alta tampoco traen edad, sexo ni pais:
    solo traen los productos, sin ningun dato del cliente."""
    return (
        F.col("ncodpers").isNotNull()
        & F.col("fecha_dato").isNotNull()
        & F.col("fecha_alta").isNotNull()
    )


def edad_valida():
    return F.coalesce(F.col("age").between(EDAD_MINIMA, EDAD_MAXIMA), F.lit(False))


def completar_con_el_mismo_cliente(df, columnas):
    """Rellena un null con el valor mas reciente que el mismo cliente tuvo en un
    mes anterior o, si no lo tuvo, el primero que tiene en un mes posterior. Un
    cliente que nunca tuvo el dato lo conserva en null."""
    cliente = Window.partitionBy("ncodpers").orderBy("fecha_dato")
    anteriores = cliente.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    posteriores = cliente.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    for c in columnas:
        df = df.withColumn(
            c,
            F.coalesce(
                F.col(c),
                F.last(c, ignorenulls=True).over(anteriores),
                F.first(c, ignorenulls=True).over(posteriores),
            ),
        )
    return df


def imputar_con_mediana(df, columna, claves):
    """Rellena los null de `columna` con la mediana de su grupo `claves`. Si el
    grupo no tiene ningun valor, usa la mediana global."""
    medianas = df.groupBy(*claves).agg(
        F.percentile_approx(columna, 0.5).alias("_mediana_grupo")
    )
    medianas = medianas.select(
        *[F.col(k).alias(f"_k_{k}") for k in claves], "_mediana_grupo"
    )
    mediana_global = df.agg(F.percentile_approx(columna, 0.5)).first()[0]
    condicion = [F.col(k).eqNullSafe(F.col(f"_k_{k}")) for k in claves]
    columnas = df.columns
    unido = df.join(F.broadcast(medianas), condicion, "left")
    relleno = F.coalesce(
        F.col(columna), F.col("_mediana_grupo"), F.lit(mediana_global)
    )
    return unido.select(
        *[relleno.alias(columna) if c == columna else F.col(c) for c in columnas]
    )


def corregir(df):
    """Corrige lo que se puede corregir sin descartar el registro."""
    # La provincia y el segmento casi no cambian: si un mes viene vacio, se toma
    # el del mismo cliente en otro mes
    df = completar_con_el_mismo_cliente(df, ["cod_prov", "segmento"])
    # Edades imposibles (126, 127, 163 y 164 anos en los datos) se tratan como
    # faltantes. Las de 100 a 117 son clientes reales y se conservan
    df = df.withColumn("edad_imputada", ~edad_valida())
    df = df.withColumn("age", F.when(edad_valida(), F.col("age")))
    df = df.withColumn("renta_imputada", F.col("renta").isNull())
    for c in COLUMNAS_DESCONOCIDO:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit("DESCONOCIDO")))
    df = imputar_con_mediana(df, "age", ["segmento"])
    df = df.withColumn("age", F.col("age").cast("int"))
    # La renta se imputa por provincia y segmento, como dice la propuesta
    return imputar_con_mediana(df, "renta", ["cod_prov", "segmento"])


def unir_provincias(df, provincias):
    """Join por cod_prov con la dimension de provincias. Es un left join: los
    clientes sin provincia se conservan con comunidad DESCONOCIDA y sin poblacion.
    Álava, Gipuzkoa y Navarra no tienen renta del INE en 2015: quedan en null."""
    unido = df.join(F.broadcast(provincias), on="cod_prov", how="left")
    for c in ["provincia", "comunidad_autonoma"]:
        unido = unido.withColumn(c, F.coalesce(F.col(c), F.lit("DESCONOCIDA")))
    return unido


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--BUCKET", required=True)
    parser.add_argument("--RAW", default="raw/santander/")
    parser.add_argument("--DIM", default="raw/dim_provincia/")
    parser.add_argument("--SILVER", default="silver/clientes/")
    # Glue agrega sus propios argumentos (--JOB_NAME, --JOB_RUN_ID, ...)
    args, _ = parser.parse_known_args()
    base = f"s3://{args.BUCKET}/"

    spark = SparkSession.builder.appName("finsir-limpieza-join").getOrCreate()
    # Spark 4 viene con ANSI activo: un cast de texto invalido seria un error y
    # aqui debe ser un null, que despues se corrige
    spark.conf.set("spark.sql.ansi.enabled", "false")

    crudo = leer_csv(spark, base + args.RAW)
    datos = normalizar(crudo).persist(StorageLevel.MEMORY_AND_DISK)

    # Una sola pasada para contar lo que se descarta y se corrige
    resumen = datos.agg(
        F.count(F.lit(1)).alias("leidas"),
        F.sum(F.when(~es_valido(), 1).otherwise(0)).alias("invalidas"),
        F.sum(F.when(es_valido() & ~edad_valida(), 1).otherwise(0)).alias("edades"),
        F.sum(F.when(es_valido() & F.col("renta").isNull(), 1).otherwise(0)).alias("rentas"),
    ).first()

    # Las medianas de la correccion recorren esta tabla varias veces
    validos = (
        datos.filter(es_valido())
        .dropDuplicates(["fecha_dato", "ncodpers"])
        .drop(*COLUMNAS_DESCARTADAS)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    limpios = corregir(validos)

    provincias = (
        spark.read.csv(base + args.DIM, header=True, quote='"')
        .select(
            F.col("cod_prov").cast("int"),
            "provincia",
            "comunidad_autonoma",
            F.col("poblacion_2016").cast("long"),
            F.col("renta_bruta_hogar_2015").cast("int"),
            F.col("renta_neta_persona_2015").cast("int"),
        )
    )
    resultado = (
        unir_provincias(limpios, provincias)
        .withColumn("num_productos", sum((F.col(p) for p in PRODUCTOS), F.lit(0)))
        .select(COLUMNAS_SILVER)
    )

    salida = base + args.SILVER
    (
        resultado.repartition("fecha_dato")
        .write.mode("overwrite")
        .partitionBy("fecha_dato")
        .parquet(salida)
    )

    escritas = spark.read.parquet(salida).count()
    duplicadas = resumen["leidas"] - resumen["invalidas"] - escritas
    print(f"[finsir] filas leidas:                    {resumen['leidas']}")
    print(f"[finsir] descartadas (sin fecha_alta):    {resumen['invalidas']}")
    print(f"[finsir] descartadas (duplicadas):        {duplicadas}")
    print(f"[finsir] edades corregidas (fuera 0-122): {resumen['edades']}")
    print(f"[finsir] rentas imputadas:                {resumen['rentas']}")
    print(f"[finsir] filas escritas en {salida}: {escritas}")
    validos.unpersist()
    datos.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
