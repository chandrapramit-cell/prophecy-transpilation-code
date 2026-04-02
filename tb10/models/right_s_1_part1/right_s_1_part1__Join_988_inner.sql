{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH aka_Server_UYDB_1116 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1_part1', 'aka_Server_UYDB_1116') }}

),

Unique_987 AS (

  {#VisualGroup: CalculoProductosMDP#}
  SELECT * 
  
  FROM aka_Server_UYDB_1116 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY codigo_producto, codigo_subproducto ORDER BY codigo_producto, codigo_subproducto) = 1

),

aka_Server_UYDB_1045 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1_part1', 'aka_Server_UYDB_1045') }}

),

aka_Server_UYDB_1031 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1_part1', 'aka_Server_UYDB_1031') }}

),

Summarize_1027 AS (

  {#VisualGroup: CalculoProductosMDP#}
  SELECT 
    SUM(cantidad_compras_totales) AS cantidad_compras_totales_3meses,
    idf_pers_ods_tarjeta AS idf_pers_ods_tarjeta,
    numero_contrato AS numero_contrato,
    pan_hash AS pan_hash
  
  FROM aka_Server_UYDB_1031 AS in0
  
  GROUP BY 
    idf_pers_ods_tarjeta, numero_contrato, pan_hash

),

aka_Server_UYDB_1030 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1_part1', 'aka_Server_UYDB_1030') }}

),

Join_1014_left_UnionLeftOuter AS (

  {#VisualGroup: CalculoProductosMDP#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods_tarjeta`, `numero_contrato`, `pan_hash`)
  
  FROM aka_Server_UYDB_1030 AS in0
  LEFT JOIN Summarize_1027 AS in1
     ON (
      (
        (in0.idf_pers_ods_tarjeta = in1.idf_pers_ods_tarjeta)
        AND (in0.numero_contrato = in1.numero_contrato)
      )
      AND (in0.pan_hash = in1.pan_hash)
    )

),

Join_1028_inner AS (

  {#VisualGroup: CalculoProductosMDP#}
  SELECT 
    in1.pan_completo AS pan,
    in0.* EXCEPT (`pan_hash`),
    in1.* EXCEPT (`numero_contrato`, `pan_completo`, `pan_hash`)
  
  FROM Join_1014_left_UnionLeftOuter AS in0
  INNER JOIN aka_Server_UYDB_1045 AS in1
     ON ((in0.numero_contrato = in1.numero_contrato) AND (in0.pan_hash = in1.pan_hash))

),

Cleanse_1016 AS (

  {#VisualGroup: CalculoProductosMDP#}
  {{
    prophecy_basics.DataCleansing(
      ['Join_1028_inner'], 
      [
        { "name": "pan", "dataType": "String" }, 
        { "name": "idf_pers_ods_tarjeta", "dataType": "String" }, 
        { "name": "motivo_baja_contrato", "dataType": "String" }, 
        { "name": "numero_contrato", "dataType": "String" }, 
        { "name": "es_titular", "dataType": "Boolean" }, 
        { "name": "bloqueo_soft_tarjeta", "dataType": "Boolean" }, 
        { "name": "idf_pers_ods_contrato", "dataType": "String" }, 
        { "name": "bloqueo_duro_contrato", "dataType": "Boolean" }, 
        { "name": "cantidad_compras_totales", "dataType": "Integer" }, 
        { "name": "fecha_baja_contrato", "dataType": "Date" }, 
        { "name": "codigo_subproducto", "dataType": "String" }, 
        { "name": "codigo_bloqueo_contrato", "dataType": "Double" }, 
        { "name": "tipo_tarjeta", "dataType": "String" }, 
        { "name": "fecha_alta_contrato", "dataType": "Date" }, 
        { "name": "limite_tarjeta", "dataType": "Decimal" }, 
        { "name": "bloqueo_soft_contrato", "dataType": "Boolean" }, 
        { "name": "condicion_economica", "dataType": "String" }, 
        { "name": "tarjeta_transaccional_mes", "dataType": "Boolean" }, 
        { "name": "codigo_producto", "dataType": "String" }, 
        { "name": "bloqueo_duro_tarjeta", "dataType": "Boolean" }, 
        { "name": "cantidad_compras_totales_3meses", "dataType": "Bigint" }
      ], 
      'keepOriginal', 
      ['cantidad_compras_totales_3meses'], 
      false, 
      '', 
      true, 
      0, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Join_988_inner AS (

  {#VisualGroup: CalculoProductosMDP#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`codigo_producto`, 
    `codigo_subproducto`, 
    `familia_producto_comercial`, 
    `grupo_producto_comercial`, 
    `tipo_producto_comercial`)
  
  FROM Cleanse_1016 AS in0
  INNER JOIN Unique_987 AS in1
     ON ((in0.codigo_producto = in1.codigo_producto) AND (in0.codigo_subproducto = in1.codigo_subproducto))

)

SELECT *

FROM Join_988_inner
