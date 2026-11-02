-- Sentencia 1: la base de datos
CREATE DATABASE IF NOT EXISTS finsir;

-- Sentencia 2: la capa bronze, el CSV de Santander tal como llego. Todas las
-- columnas son texto: no se rechaza ni se cambia nada al leerlo.
CREATE EXTERNAL TABLE finsir.bronze_santander (
    fecha_dato            string,
    ncodpers              string,
    ind_empleado          string,
    pais_residencia       string,
    sexo                  string,
    age                   string,
    fecha_alta            string,
    ind_nuevo             string,
    antiguedad            string,
    indrel                string,
    ult_fec_cli_1t        string,
    indrel_1mes           string,
    tiprel_1mes           string,
    indresi               string,
    indext                string,
    conyuemp              string,
    canal_entrada         string,
    indfall               string,
    tipodom               string,
    cod_prov              string,
    nomprov               string,
    ind_actividad_cliente string,
    renta                 string,
    segmento              string,
    ind_ahor_fin_ult1     string,
    ind_aval_fin_ult1     string,
    ind_cco_fin_ult1      string,
    ind_cder_fin_ult1     string,
    ind_cno_fin_ult1      string,
    ind_ctju_fin_ult1     string,
    ind_ctma_fin_ult1     string,
    ind_ctop_fin_ult1     string,
    ind_ctpp_fin_ult1     string,
    ind_deco_fin_ult1     string,
    ind_deme_fin_ult1     string,
    ind_dela_fin_ult1     string,
    ind_ecue_fin_ult1     string,
    ind_fond_fin_ult1     string,
    ind_hip_fin_ult1      string,
    ind_plan_fin_ult1     string,
    ind_pres_fin_ult1     string,
    ind_reca_fin_ult1     string,
    ind_tjcr_fin_ult1     string,
    ind_valo_fin_ult1     string,
    ind_viv_fin_ult1      string,
    ind_nomina_ult1       string,
    ind_nom_pens_ult1     string,
    ind_recibo_ult1       string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://finsir-trabajo2-593077778483/raw/santander/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Sentencia 3: el segundo conjunto de datos, con el que se hace el join
CREATE EXTERNAL TABLE finsir.bronze_dim_provincia (
    cod_prov                string,
    provincia               string,
    comunidad_autonoma      string,
    poblacion_2016          string,
    renta_bruta_hogar_2015  string,
    renta_neta_persona_2015 string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://finsir-trabajo2-593077778483/raw/dim_provincia/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Sentencia 4: la capa silver, los datos transformados por el trabajo inicial
CREATE EXTERNAL TABLE finsir.silver_clientes (
    ncodpers              bigint,
    ind_empleado          string,
    pais_residencia       string,
    sexo                  string,
    age                   int,
    fecha_alta            date,
    ind_nuevo             int,
    antiguedad            int,
    indrel                int,
    indrel_1mes           string,
    tiprel_1mes           string,
    indresi               string,
    indext                string,
    canal_entrada         string,
    indfall               string,
    cod_prov              int,
    ind_actividad_cliente int,
    renta                 double,
    segmento              string,
    ind_ahor_fin_ult1     int,
    ind_aval_fin_ult1     int,
    ind_cco_fin_ult1      int,
    ind_cder_fin_ult1     int,
    ind_cno_fin_ult1      int,
    ind_ctju_fin_ult1     int,
    ind_ctma_fin_ult1     int,
    ind_ctop_fin_ult1     int,
    ind_ctpp_fin_ult1     int,
    ind_deco_fin_ult1     int,
    ind_deme_fin_ult1     int,
    ind_dela_fin_ult1     int,
    ind_ecue_fin_ult1     int,
    ind_fond_fin_ult1     int,
    ind_hip_fin_ult1      int,
    ind_plan_fin_ult1     int,
    ind_pres_fin_ult1     int,
    ind_reca_fin_ult1     int,
    ind_tjcr_fin_ult1     int,
    ind_valo_fin_ult1     int,
    ind_viv_fin_ult1      int,
    ind_nomina_ult1       int,
    ind_nom_pens_ult1     int,
    ind_recibo_ult1       int,
    edad_imputada         boolean,
    renta_imputada        boolean,
    provincia             string,
    comunidad_autonoma    string,
    poblacion_2016        bigint,
    renta_bruta_hogar_2015  int,
    renta_neta_persona_2015 int,
    num_productos         int
)
PARTITIONED BY (fecha_dato string)
STORED AS PARQUET
LOCATION 's3://finsir-trabajo2-593077778483/silver/clientes/';

-- Sentencia 5: registra las particiones que dejo el trabajo de Glue, una por mes
MSCK REPAIR TABLE finsir.silver_clientes;

-- Sentencia 6: la agrupacion de la primera rama
CREATE EXTERNAL TABLE finsir.gold_altas_producto_mes (
    fecha_dato            date,
    producto              string,
    nombre_producto       string,
    clientes_con_producto bigint,
    altas                 bigint,
    bajas                 bigint
)
STORED AS PARQUET
LOCATION 's3://finsir-trabajo2-593077778483/gold/altas_producto_mes/';

-- Sentencia 7: la agrupacion de la segunda rama
CREATE EXTERNAL TABLE finsir.gold_penetracion_provincia (
    fecha_dato              date,
    cod_prov                int,
    provincia               string,
    comunidad_autonoma      string,
    poblacion_2016          bigint,
    renta_neta_persona_2015 int,
    clientes                bigint,
    productos_promedio      double,
    pct_nomina              double,
    pct_tarjeta_credito     double,
    clientes_por_mil_hab    double,
    brecha_clientes         bigint
)
STORED AS PARQUET
LOCATION 's3://finsir-trabajo2-593077778483/gold/penetracion_provincia/';
