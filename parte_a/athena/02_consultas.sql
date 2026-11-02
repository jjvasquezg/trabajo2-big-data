-- Sentencia 1: los datos de partida. Expected: 13647309 filas, 17 meses
SELECT count(*) AS filas, count(DISTINCT fecha_dato) AS meses
FROM bronze_santander;

-- Sentencia 2: filas por particion de la capa silver, una por mes. El filtro
-- por fecha_dato lee solo la carpeta de ese mes.
SELECT fecha_dato, count(*) AS clientes
FROM silver_clientes
GROUP BY fecha_dato
ORDER BY fecha_dato;

-- Sentencia 3: el total de la capa silver
SELECT count(*) AS filas, count(DISTINCT ncodpers) AS clientes_distintos
FROM silver_clientes;

-- Sentencia 4: lo que hizo la limpieza. La edad y la renta imputadas llevan
-- una marca para que se pueda ver que se corrigio.
SELECT sum(CASE WHEN edad_imputada THEN 1 ELSE 0 END) AS edades_corregidas,
       sum(CASE WHEN renta_imputada THEN 1 ELSE 0 END) AS rentas_imputadas,
       sum(CASE WHEN comunidad_autonoma = 'DESCONOCIDA' THEN 1 ELSE 0 END) AS sin_provincia
FROM silver_clientes;

-- =========================================================================
-- Primera rama. Pregunta: cuantas contrataciones nuevas (altas) tiene cada
-- producto en cada mes, y cuales son los productos que mas se contratan?
-- =========================================================================

-- Sentencia 5: la agrupacion completa tiene una fila por mes y producto.
-- Las altas de 2015-01-28 son null: no hay mes anterior con que comparar.
SELECT count(*) AS filas, count(DISTINCT producto) AS productos,
       count(DISTINCT fecha_dato) AS meses
FROM gold_altas_producto_mes;

-- Sentencia 6: los productos que mas se contratan en todo el periodo
SELECT nombre_producto, sum(altas) AS altas_totales, sum(bajas) AS bajas_totales
FROM gold_altas_producto_mes
GROUP BY nombre_producto
ORDER BY altas_totales DESC
LIMIT 10;

-- Sentencia 7: los 3 productos mas contratados en cada mes
SELECT fecha_dato, nombre_producto, altas
FROM (
    SELECT fecha_dato, nombre_producto, altas,
           row_number() OVER (PARTITION BY fecha_dato ORDER BY altas DESC) AS posicion
    FROM gold_altas_producto_mes
    WHERE altas IS NOT NULL
)
WHERE posicion <= 3
ORDER BY fecha_dato, posicion;

-- =========================================================================
-- Segunda rama. Pregunta: en que provincias tiene el banco menos clientes en
-- relacion con su poblacion, y cuantos clientes le faltan para llegar a la
-- penetracion tipica (la mediana de las provincias)?
-- Los clientes del archivo son una muestra: la penetracion compara provincias
-- entre si, no es una cuota de mercado.
-- =========================================================================

-- Sentencia 8: el corte que se resumio. Hay una fila por provincia y una para
-- los clientes sin provincia.
SELECT fecha_dato, count(*) AS filas, sum(clientes) AS clientes
FROM gold_penetracion_provincia
GROUP BY fecha_dato;

-- Sentencia 9: las 10 provincias a las que mas clientes les faltan
SELECT provincia, poblacion_2016, clientes, clientes_por_mil_hab, brecha_clientes
FROM gold_penetracion_provincia
ORDER BY brecha_clientes DESC
LIMIT 10;

-- Sentencia 10: las 3 provincias con mas y las 3 con menos clientes por cada mil
-- habitantes
(SELECT provincia, poblacion_2016, clientes, clientes_por_mil_hab
 FROM gold_penetracion_provincia
 WHERE clientes_por_mil_hab IS NOT NULL
 ORDER BY clientes_por_mil_hab DESC
 LIMIT 3)
UNION ALL
(SELECT provincia, poblacion_2016, clientes, clientes_por_mil_hab
 FROM gold_penetracion_provincia
 WHERE clientes_por_mil_hab IS NOT NULL
 ORDER BY clientes_por_mil_hab ASC
 LIMIT 3);

-- Sentencia 11: la misma brecha, pero solo en provincias con renta neta por
-- persona de 10000 euros o mas: ahi cada cliente nuevo vale mas. El INE no
-- publica la renta de 2015 de Alava, Gipuzkoa ni Navarra: no entran.
SELECT provincia, renta_neta_persona_2015, clientes_por_mil_hab, brecha_clientes
FROM gold_penetracion_provincia
WHERE renta_neta_persona_2015 >= 10000 AND brecha_clientes > 0
ORDER BY brecha_clientes DESC
LIMIT 8;

-- Sentencia 12: la brecha sumada por comunidad autonoma
SELECT comunidad_autonoma, sum(clientes) AS clientes, sum(poblacion_2016) AS poblacion,
       round(1000.0 * sum(clientes) / sum(poblacion_2016), 2) AS clientes_por_mil_hab,
       sum(brecha_clientes) AS brecha_clientes
FROM gold_penetracion_provincia
WHERE poblacion_2016 IS NOT NULL
GROUP BY comunidad_autonoma
ORDER BY brecha_clientes DESC
LIMIT 6;

-- =========================================================================
-- Por que se particiona por fecha_dato: la misma pregunta sobre el CSV y sobre
-- la capa silver. Mira el Data scanned que Athena muestra bajo cada resultado.
-- =========================================================================

-- Sentencia 13: los clientes de marzo de 2016 en el CSV (bronze). Athena lee los
-- 2.3 GB, porque no puede saltarse nada.
SELECT count(*) AS clientes
FROM bronze_santander
WHERE fecha_dato = '2016-03-28';

-- Sentencia 14: lo mismo en la capa silver. El filtro sobre la columna de
-- particion hace que Athena lea solo la carpeta fecha_dato=2016-03-28, y solo la
-- columna que necesita. Expected: 925076 clientes, con un Data scanned mucho
-- menor que el de la sentencia 13.
SELECT count(*) AS clientes
FROM silver_clientes
WHERE fecha_dato = '2016-03-28';

-- =========================================================================
-- Que queda vacio despues de la limpieza, y por que
-- =========================================================================

-- Sentencia 15: los null que quedan en la capa silver. La limpieza corrige lo
-- que se puede corregir con una regla defendible (edad, renta, textos vacios) y
-- no inventa lo que no se sabe: un cliente sin provincia no tiene poblacion, y
-- el INE no publica la renta de 2015 de Alava, Gipuzkoa ni Navarra.
-- Expected: 0 en todas las columnas menos las cuatro de provincia. El segmento y
-- la provincia se completan con los de otro mes del mismo cliente; lo que no se
-- puede recuperar queda como DESCONOCIDO (segmento) o null (provincia).
SELECT count(*) AS filas,
       count_if(age IS NULL) AS age,
       count_if(renta IS NULL) AS renta,
       count_if(sexo IS NULL OR sexo = '') AS sexo,
       count_if(segmento IS NULL OR segmento = '') AS segmento,
       count_if(cod_prov IS NULL) AS cod_prov,
       count_if(poblacion_2016 IS NULL) AS poblacion_2016,
       count_if(renta_bruta_hogar_2015 IS NULL) AS renta_bruta_hogar_2015,
       count_if(renta_neta_persona_2015 IS NULL) AS renta_neta_persona_2015
FROM silver_clientes;

-- Sentencia 16: los null que quedan en las dos agrupaciones
-- Expected: 24 meses-producto sin altas ni bajas (los 24 productos de
-- 2015-01-28) y, en la segunda, 1 fila sin provincia y 4 sin renta.
SELECT 'gold_altas_producto_mes' AS tabla,
       count_if(altas IS NULL) AS sin_altas, count_if(bajas IS NULL) AS sin_bajas,
       0 AS sin_provincia, 0 AS sin_renta
FROM gold_altas_producto_mes
UNION ALL
SELECT 'gold_penetracion_provincia', 0, 0,
       count_if(cod_prov IS NULL), count_if(renta_neta_persona_2015 IS NULL)
FROM gold_penetracion_provincia;
