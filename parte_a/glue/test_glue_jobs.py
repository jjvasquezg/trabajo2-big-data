import importlib.util
import os
import sys
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

# En Windows los trabajadores de Spark necesitan saber cual es el Python
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def cargar(nombre):
    """Importa un script cuyo nombre empieza por numero, como 01_limpieza_join."""
    ruta = Path(__file__).parent / f"{nombre}.py"
    spec = importlib.util.spec_from_file_location(nombre, ruta)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


limpieza = cargar("01_limpieza_join")
altas = cargar("02_rama_altas_producto_mes")
penetracion = cargar("03_rama_penetracion_provincia")


@pytest.fixture(scope="module")
def spark():
    sesion = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    # Lo mismo que hace main() en cada script
    sesion.conf.set("spark.sql.ansi.enabled", "false")
    yield sesion
    sesion.stop()


def crudo(spark, filas):
    """Un DataFrame con las 48 columnas del CSV, todas en texto. Lo que no se
    indica queda vacio, como llega en el archivo."""
    esquema = StructType([StructField(c, StringType()) for c in limpieza.COLUMNAS])
    return spark.createDataFrame(
        [tuple(f.get(c, "") for c in limpieza.COLUMNAS) for f in filas], esquema
    )


def cliente(**campos):
    base = {
        "fecha_dato": "2015-01-28", "ncodpers": "1", "fecha_alta": "2014-05-10",
        "age": " 35", "segmento": "02 - PARTICULARES", "cod_prov": "28",
        "renta": "100.0", "indrel_1mes": "1.0",
    }
    return {**base, **campos}


def test_normalizar_quita_espacios_vacios_y_na(spark):
    fila = limpieza.normalizar(crudo(spark, [
        cliente(age="  40", ind_nuevo=" 0", antiguedad="NA", ind_nomina_ult1="NA",
                ind_cco_fin_ult1="1", sexo="  "),
    ])).first()
    assert fila["age"] == 40
    assert fila["ind_nuevo"] == 0
    assert fila["antiguedad"] is None
    assert fila["sexo"] is None
    assert fila["indrel_1mes"] == "1"
    assert fila["fecha_dato"] == date(2015, 1, 28)
    # un producto sin dato es un producto que no se tiene
    assert fila["ind_nomina_ult1"] == 0
    assert fila["ind_cco_fin_ult1"] == 1


def test_registro_sin_fecha_alta_es_invalido(spark):
    df = limpieza.normalizar(crudo(spark, [
        cliente(ncodpers="1"),
        cliente(ncodpers="2", fecha_alta=""),
        cliente(ncodpers="3", fecha_alta="no es una fecha"),
    ]))
    validos = [r["ncodpers"] for r in df.filter(limpieza.es_valido()).collect()]
    assert validos == [1]


def test_completar_con_el_mismo_cliente(spark):
    df = limpieza.normalizar(crudo(spark, [
        # el segmento y la provincia llegan en el segundo mes: se copian hacia atras
        cliente(ncodpers="1", fecha_dato="2015-01-28", segmento="", cod_prov=""),
        cliente(ncodpers="1", fecha_dato="2015-02-28", segmento="01 - TOP", cod_prov="28"),
        # y un mes vacio entre dos con dato toma el anterior
        cliente(ncodpers="2", fecha_dato="2015-01-28", segmento="03 - UNIVERSITARIO", cod_prov="8"),
        cliente(ncodpers="2", fecha_dato="2015-02-28", segmento="", cod_prov=""),
        cliente(ncodpers="2", fecha_dato="2015-03-28", segmento="02 - PARTICULARES", cod_prov="8"),
        # un cliente que nunca tuvo el dato lo conserva en null
        cliente(ncodpers="3", segmento="", cod_prov=""),
    ]))
    filas = {(r["ncodpers"], r["fecha_dato"].month): r
             for r in limpieza.completar_con_el_mismo_cliente(df, ["cod_prov", "segmento"]).collect()}
    assert (filas[(1, 1)]["segmento"], filas[(1, 1)]["cod_prov"]) == ("01 - TOP", 28)
    assert (filas[(2, 2)]["segmento"], filas[(2, 2)]["cod_prov"]) == ("03 - UNIVERSITARIO", 8)
    assert filas[(3, 1)]["segmento"] is None and filas[(3, 1)]["cod_prov"] is None


def test_corregir_imputa_edad_renta_y_categorias(spark):
    df = limpieza.normalizar(crudo(spark, [
        cliente(ncodpers="1", age="30", renta="100"),
        cliente(ncodpers="2", age="40", renta="300"),
        cliente(ncodpers="3", age="164", renta=""),
        cliente(ncodpers="4", age="35", renta="", cod_prov="8", segmento="03 - UNIVERSITARIO"),
        cliente(ncodpers="5", age="22", renta="50", cod_prov="8", segmento="03 - UNIVERSITARIO", sexo="V"),
    ]))
    filas = {r["ncodpers"]: r for r in limpieza.corregir(df).collect()}
    # 164 anos no es una edad: se reemplaza por la mediana de su segmento
    assert filas[3]["edad_imputada"] and filas[3]["age"] in (30, 35, 40)
    assert not filas[1]["edad_imputada"] and filas[1]["age"] == 30
    # la renta vacia toma la mediana de su provincia y segmento
    assert filas[3]["renta_imputada"] and filas[3]["renta"] in (100.0, 300.0)
    assert filas[4]["renta"] == 50.0
    assert filas[1]["renta"] == 100.0
    # texto vacio pasa a DESCONOCIDO
    assert filas[1]["sexo"] == "DESCONOCIDO" and filas[5]["sexo"] == "V"


def test_join_conserva_clientes_sin_provincia(spark):
    df = limpieza.normalizar(crudo(spark, [
        cliente(ncodpers="1", cod_prov="28"),
        cliente(ncodpers="2", cod_prov=""),
        cliente(ncodpers="3", cod_prov="99"),
    ]))
    provincias = spark.createDataFrame(
        [(28, "Madrid", "Comunidad de Madrid", 6466996)],
        ["cod_prov", "provincia", "comunidad_autonoma", "poblacion_2016"],
    )
    filas = {r["ncodpers"]: r for r in limpieza.unir_provincias(df, provincias).collect()}
    assert len(filas) == 3
    assert filas[1]["comunidad_autonoma"] == "Comunidad de Madrid"
    assert filas[1]["poblacion_2016"] == 6466996 and filas[2]["poblacion_2016"] is None
    assert filas[2]["comunidad_autonoma"] == "DESCONOCIDA"
    assert filas[3]["provincia"] == "DESCONOCIDA"


def test_altas_solo_cuentan_contra_el_mes_anterior(spark):
    p = "ind_cco_fin_ult1"
    base = {q: 0 for q in altas.PRODUCTOS}
    filas = [
        # A: no tenia el producto en enero y lo tiene en febrero -> 1 alta
        (1, date(2015, 1, 28), {**base, p: 0}),
        (1, date(2015, 2, 28), {**base, p: 1}),
        # B: aparece por primera vez en febrero con el producto -> no es alta
        (2, date(2015, 2, 28), {**base, p: 1}),
        # C: tenia el producto en enero y lo pierde en febrero -> 1 baja
        (3, date(2015, 1, 28), {**base, p: 1}),
        (3, date(2015, 2, 28), {**base, p: 0}),
        # D: enero y marzo, sin febrero -> el hueco no cuenta como alta
        (4, date(2015, 1, 28), {**base, p: 0}),
        (4, date(2015, 3, 28), {**base, p: 1}),
    ]
    df = spark.createDataFrame(
        [(i, f, *[v[q] for q in altas.PRODUCTOS]) for i, f, v in filas],
        ["ncodpers", "fecha_dato", *altas.PRODUCTOS],
    )
    resultado = {
        (r["fecha_dato"], r["producto"]): r
        for r in altas.calcular_altas(df).collect()
    }
    assert len(resultado) == 3 * len(altas.PRODUCTOS)
    enero = resultado[(date(2015, 1, 28), p)]
    febrero = resultado[(date(2015, 2, 28), p)]
    marzo = resultado[(date(2015, 3, 28), p)]
    assert enero["altas"] is None
    assert enero["clientes_con_producto"] == 1
    assert (febrero["altas"], febrero["bajas"], febrero["clientes_con_producto"]) == (1, 1, 2)
    assert (marzo["altas"], marzo["bajas"]) == (None, None)


def test_penetracion_y_brecha_por_provincia(spark):
    columnas = ["fecha_dato", "cod_prov", "provincia", "comunidad_autonoma", "poblacion_2016",
                "renta_neta_persona_2015", "num_productos", "ind_nomina_ult1", "ind_tjcr_fin_ult1"]
    corte = date(2016, 5, 28)
    filas = (
        # A: 4 clientes en 1000 habitantes, 2 de ellos con nomina
        [(corte, 1, "A", "X", 1000, 9000, 2, 1, 0)] * 2
        + [(corte, 1, "A", "X", 1000, 9000, 4, 0, 1)] * 2
        # B: 10 clientes en 1000 habitantes
        + [(corte, 2, "B", "X", 1000, 9000, 1, 0, 0)] * 10
        # C: 16 clientes en 1000 habitantes
        + [(corte, 3, "C", "Y", 1000, None, 1, 0, 0)] * 16
        # clientes sin provincia: se cuentan, pero no tienen penetracion
        + [(corte, None, "DESCONOCIDA", "DESCONOCIDA", None, None, 1, 0, 0)] * 3
    )
    df = spark.createDataFrame(filas, columnas)
    resultado = {r["provincia"]: r for r in penetracion.calcular_penetracion(df).collect()}
    a = resultado["A"]
    assert (a["clientes"], a["clientes_por_mil_hab"], a["productos_promedio"]) == (4, 4.0, 3.0)
    assert a["pct_nomina"] == 50.0 and a["pct_tarjeta_credito"] == 50.0
    # la mediana de 4, 10 y 16 clientes por mil habitantes es 10: A le faltan 6
    assert a["brecha_clientes"] == 6
    assert resultado["B"]["brecha_clientes"] == 0
    assert resultado["C"]["brecha_clientes"] == 0
    assert resultado["DESCONOCIDA"]["clientes"] == 3
    assert resultado["DESCONOCIDA"]["clientes_por_mil_hab"] is None
