import argparse
import statistics
from datetime import date

from pyspark.sql import SparkSession, functions as F

COLUMNAS_PROVINCIA = [
    "cod_prov", "provincia", "comunidad_autonoma", "poblacion_2016",
    "renta_neta_persona_2015",
]


def calcular_penetracion(df):
    """Una fila por provincia con sus clientes, su penetracion por mil
    habitantes y la brecha: los clientes que le faltan para llegar a la
    penetracion mediana de las provincias."""
    por_provincia = df.groupBy("fecha_dato", *COLUMNAS_PROVINCIA).agg(
        F.count(F.lit(1)).alias("clientes"),
        F.round(F.avg("num_productos"), 3).alias("productos_promedio"),
        F.round(100 * F.avg("ind_nomina_ult1"), 2).alias("pct_nomina"),
        F.round(100 * F.avg("ind_tjcr_fin_ult1"), 2).alias("pct_tarjeta_credito"),
    )
    por_provincia = por_provincia.withColumn(
        "clientes_por_mil_hab",
        F.round(1000 * F.col("clientes") / F.col("poblacion_2016"), 2),
    )
    # Son 52 provincias: la mediana exacta se calcula en el driver
    valores = [
        r[0] for r in por_provincia
        .where(F.col("clientes_por_mil_hab").isNotNull())
        .select(1000 * F.col("clientes") / F.col("poblacion_2016"))
        .collect()
    ]
    mediana = statistics.median(valores)
    brecha = mediana * F.col("poblacion_2016") / 1000 - F.col("clientes")
    return por_provincia.withColumn(
        "brecha_clientes", F.round(F.greatest(brecha, F.lit(0.0)), 0).cast("long")
    )


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--BUCKET", required=True)
    parser.add_argument("--SILVER", default="silver/clientes/")
    parser.add_argument("--GOLD", default="gold/penetracion_provincia/")
    parser.add_argument("--CORTE", default="ultimo")
    args, _ = parser.parse_known_args()
    base = f"s3://{args.BUCKET}/"

    spark = SparkSession.builder.appName("finsir-rama-penetracion-provincia").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")

    silver = spark.read.parquet(base + args.SILVER).withColumn(
        "fecha_dato", F.col("fecha_dato").cast("date")
    )
    if args.CORTE == "ultimo":
        corte = silver.agg(F.max("fecha_dato")).first()[0]
    else:
        corte = date.fromisoformat(args.CORTE)
    resultado = calcular_penetracion(silver.filter(F.col("fecha_dato") == F.lit(corte)))

    salida = base + args.GOLD
    resultado.coalesce(1).write.mode("overwrite").parquet(salida)
    print(f"[finsir] corte {corte}: {spark.read.parquet(salida).count()} filas en {salida}")
    spark.stop()


if __name__ == "__main__":
    main()
