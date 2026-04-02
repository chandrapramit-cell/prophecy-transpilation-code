{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_AlteryxSelect_1241",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH aka_Server_UYDB_1046 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('productos_part1', 'aka_Server_UYDB_1046') }}

),

Filter_1242 AS (

  {#VisualGroup: GetnetProd34#}
  SELECT * 
  
  FROM aka_Server_UYDB_1046 AS in0
  
  WHERE (
          (fecha_ultima_transaccion >= to_date(add_months(current_date(), -3)))
          AND NOT ((isnull(idf_pers_ods) OR (length(idf_pers_ods) = 0)))
        )

),

Summarize_1244 AS (

  {#VisualGroup: GetnetProd34#}
  SELECT 
    MIN(fecha_activacion) AS fecha_activacion,
    MIN(fecha_primera_transaccion) AS fecha_primera_transaccion,
    MAX(fecha_ultima_transaccion) AS fecha_ultima_transaccion,
    SUM(monto_transacciones_mes_actual) AS monto_transacciones_mes_actual,
    banco_cuenta AS banco_cuenta,
    idf_pers_ods AS idf_pers_ods
  
  FROM Filter_1242 AS in0
  
  GROUP BY 
    banco_cuenta, idf_pers_ods

),

Summarize_1243 AS (

  {#VisualGroup: GetnetProd34#}
  SELECT 
    SUM(monto_transacciones_mes_actual) AS monto_transacciones_mes_actual,
    concat_ws(',', collect_list(banco_cuenta)) AS banco_cuenta,
    first(fecha_activacion) AS fecha_activacion,
    first(fecha_primera_transaccion) AS fecha_primera_transaccion,
    first(fecha_ultima_transaccion) AS fecha_ultima_transaccion,
    idf_pers_ods AS idf_pers_ods
  
  FROM Summarize_1244 AS in0
  
  GROUP BY idf_pers_ods

),

Formula_1240_0 AS (

  {#VisualGroup: GetnetProd34#}
  SELECT 
    CAST(INITCAP(banco_cuenta) AS string) AS banco_cuenta,
    CAST(34 AS INTEGER) AS CodProducto,
    * EXCEPT (`banco_cuenta`)
  
  FROM Summarize_1243 AS in0

),

Formula_1240_1 AS (

  {#VisualGroup: GetnetProd34#}
  SELECT 
    CAST((CONCAT('Banco cobro: ', banco_cuenta)) AS string) AS Col1,
    CAST((CONCAT('Fecha activacion: ', (DATE_FORMAT(fecha_activacion, 'dd-MM-yyyy')))) AS string) AS Col2,
    CAST((CONCAT('Fecha primera transaccion: ', (DATE_FORMAT(fecha_primera_transaccion, 'dd-MM-yyyy')))) AS string) AS Col3,
    CAST((CONCAT('Fecha ultima transaccion: ', (DATE_FORMAT(fecha_ultima_transaccion, 'dd-MM-yyyy')))) AS string) AS Col4,
    CAST((
      CONCAT(
        'Monto transacciones mes (Arbitrado UYU): ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(monto_transacciones_mes_actual AS DOUBLE), 2)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col5,
    *
  
  FROM Formula_1240_0 AS in0

),

AlteryxSelect_1241 AS (

  {#VisualGroup: GetnetProd34#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5
  
  FROM Formula_1240_1 AS in0

)

SELECT *

FROM AlteryxSelect_1241
