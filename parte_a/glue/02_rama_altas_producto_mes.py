import argparse

from pyspark.sql import SparkSession, Window, functions as F

PRODUCTOS = {
    "ind_ahor_fin_ult1": "Ahorro",
    "ind_aval_fin_ult1": "Avales",
    "ind_cco_fin_ult1": "Cuenta corriente",
    "ind_cder_fin_ult1": "Derivados",
    "ind_cno_fin_ult1": "Nómina",
    "ind_ctju_fin_ult1": "Cuenta juvenil",
    "ind_ctma_fin_ult1": "Cuenta más particular",
    "ind_ctop_fin_ult1": "Cuenta particular",
    "ind_ctpp_fin_ult1": "Cuenta particular Plus",
    "ind_deco_fin_ult1": "Depósitos corto plazo",
    "ind_deme_fin_ult1": "Depósitos mediano plazo",
    "ind_dela_fin_ult1": "Depósitos largo plazo",
    "ind_ecue_fin_ult1": "E-cuenta",
    "ind_fond_fin_ult1": "Fondos de inversión",
    "ind_hip_fin_ult1": "Hipoteca",
    "ind_plan_fin_ult1": "Planes de pensiones",
    "ind_pres_fin_ult1": "Préstamos",
    "ind_reca_fin_ult1": "Impuestos",
    "ind_tjcr_fin_ult1": "Tarjeta de crédito",
    "ind_valo_fin_ult1": "Valores",
    "ind_viv_fin_ult1": "Vivienda",
    "ind_nomina_ult1": "Nómina ingreso",
    "ind_nom_pens_ult1": "Pensión",
    "ind_recibo_ult1": "Débito automático",
}


def calcular_altas(df):
    """Una fila por mes y producto con los clientes que lo tienen, las altas
    (lo tienen y el mes anterior no) y las bajas (lo tenian y ya no).

    Solo se compara contra el mes inmediatamente anterior: un cliente que no
    aparece el mes anterior (cliente nuevo o con un hueco) no cuenta. Por eso
    las altas del primer mes de los datos son null: no hay mes anterior."""
    ventana = Window.partitionBy("ncodpers").orderBy("fecha_dato")
    df = df.withColumn("_fecha_prev", F.lag("fecha_dato").over(ventana))
    for p in PRODUCTOS:
        df = df.withColumn(f"_{p}_prev", F.lag(p).over(ventana))

    es_mes_anterior = F.col("_fecha_prev") == F.add_months(F.col("fecha_dato"), -1)
    agregados = []
    for p in PRODUCTOS:
        antes, ahora = F.col(f"_{p}_prev"), F.col(p)
        agregados += [
            F.sum(ahora).alias(f"tiene_{p}"),
            F.sum(F.when(es_mes_anterior, ((antes == 0) & (ahora == 1)).cast("int"))).alias(f"alta_{p}"),
            F.sum(F.when(es_mes_anterior, ((antes == 1) & (ahora == 0)).cast("int"))).alias(f"baja_{p}"),
        ]
    ancho = df.groupBy("fecha_dato").agg(*agregados)

    # De una fila por mes con 72 columnas a una fila por mes y producto
    pares = ", ".join(f"'{p}', tiene_{p}, alta_{p}, baja_{p}" for p in PRODUCTOS)
    return ancho.selectExpr(
        "fecha_dato",
        f"stack({len(PRODUCTOS)}, {pares}) as (producto, clientes_con_producto, altas, bajas)",
    )


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--BUCKET", required=True)
    parser.add_argument("--SILVER", default="silver/clientes/")
    parser.add_argument("--GOLD", default="gold/altas_producto_mes/")
    args, _ = parser.parse_known_args()
    base = f"s3://{args.BUCKET}/"

    spark = SparkSession.builder.appName("finsir-rama-altas-producto-mes").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")

    silver = (
        spark.read.parquet(base + args.SILVER)
        .select("ncodpers", F.col("fecha_dato").cast("date").alias("fecha_dato"), *PRODUCTOS)
    )
    nombres = spark.createDataFrame(list(PRODUCTOS.items()), ["producto", "nombre_producto"])
    resultado = (
        calcular_altas(silver)
        .join(F.broadcast(nombres), on="producto")
        .select("fecha_dato", "producto", "nombre_producto", "clientes_con_producto", "altas", "bajas")
    )

    salida = base + args.GOLD
    resultado.coalesce(1).write.mode("overwrite").parquet(salida)
    print(f"[finsir] filas escritas en {salida}: {spark.read.parquet(salida).count()}")
    spark.stop()


if __name__ == "__main__":
    main()
