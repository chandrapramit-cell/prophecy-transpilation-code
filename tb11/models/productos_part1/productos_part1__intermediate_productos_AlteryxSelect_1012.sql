{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_AlteryxSelect_1012",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_996_0 AS (

  SELECT *
  
  FROM {{ ref('productos_part1__Formula_996_0')}}

),

Filter_1008 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT * 
  
  FROM Formula_996_0 AS in0
  
  WHERE ((codigo_producto = '88') AND (CAST(codigo_subproducto AS string) IN ('5601', '7601', '7603')))

),

Join_988_inner AS (

  SELECT *
  
  FROM {{ ref('productos_part1__Join_988_inner')}}

),

Filter_1004 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT * 
  
  FROM Join_988_inner AS in0
  
  WHERE (
          (codigo_producto = '80')
          AND (CAST(codigo_subproducto AS string) IN ('3096', '3196', '3098', '3005'))
        )

),

Summarize_1001 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT DISTINCT idf_pers_ods_tarjeta AS idf_pers_ods
  
  FROM Filter_1004 AS in0

),

Filter_1005 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT * 
  
  FROM Join_988_inner AS in0
  
  WHERE (
          (codigo_producto = '77')
          AND (CAST(codigo_subproducto AS string) IN ('3095', '3195', '3097', '3004'))
        )

),

Summarize_1002 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT DISTINCT idf_pers_ods_tarjeta AS idf_pers_ods
  
  FROM Filter_1005 AS in0

),

Filter_1003 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT * 
  
  FROM Join_988_inner AS in0
  
  WHERE ((codigo_producto = '88') AND (CAST(codigo_subproducto AS string) IN ('5601', '7601', '7603')))

),

Summarize_1000 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT DISTINCT idf_pers_ods_tarjeta AS idf_pers_ods
  
  FROM Filter_1003 AS in0

),

JoinMultiple_999 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT in0.idf_pers_ods AS idf_pers_ods
  
  FROM Summarize_1001 AS in0
  INNER JOIN Summarize_1002 AS in1
     ON (in0.idf_pers_ods = in1.idf_pers_ods)
  INNER JOIN Summarize_1000 AS in2
     ON (coalesce(in0.idf_pers_ods, in1.idf_pers_ods) = in2.idf_pers_ods)

),

Join_1009_inner AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods`)
  
  FROM Filter_1008 AS in0
  INNER JOIN JoinMultiple_999 AS in1
     ON (in0.idf_pers_ods_tarjeta = in1.idf_pers_ods)

),

Formula_990_0 AS (

  SELECT *
  
  FROM {{ ref('productos_part1__Formula_990_0')}}

),

Filter_1007 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT * 
  
  FROM Formula_990_0 AS in0
  
  WHERE (
          (
            (
              (codigo_producto = '77')
              AND (CAST(codigo_subproducto AS string) IN ('3095', '3195', '3097', '3004'))
            )
            OR (codigo_producto = '80')
          )
          AND (CAST(codigo_subproducto AS string) IN ('3096', '3196', '3098', '3005'))
        )

),

Join_1006_inner AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods`)
  
  FROM Filter_1007 AS in0
  INNER JOIN JoinMultiple_999 AS in1
     ON (in0.idf_pers_ods_tarjeta = in1.idf_pers_ods)

),

Union_1010 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  {{
    prophecy_basics.UnionByName(
      ['Join_1006_inner', 'Join_1009_inner'], 
      [
        '[{"name": "CodProducto", "dataType": "Integer"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "pan", "dataType": "String"}, {"name": "idf_pers_ods_tarjeta", "dataType": "String"}, {"name": "motivo_baja_contrato", "dataType": "String"}, {"name": "numero_contrato", "dataType": "String"}, {"name": "es_titular", "dataType": "Boolean"}, {"name": "bloqueo_soft_tarjeta", "dataType": "Boolean"}, {"name": "idf_pers_ods_contrato", "dataType": "String"}, {"name": "bloqueo_duro_contrato", "dataType": "Boolean"}, {"name": "cantidad_compras_totales", "dataType": "Integer"}, {"name": "fecha_baja_contrato", "dataType": "Date"}, {"name": "codigo_subproducto", "dataType": "String"}, {"name": "codigo_bloqueo_contrato", "dataType": "Double"}, {"name": "tipo_tarjeta", "dataType": "String"}, {"name": "fecha_alta_contrato", "dataType": "Date"}, {"name": "limite_tarjeta", "dataType": "Decimal"}, {"name": "bloqueo_soft_contrato", "dataType": "Boolean"}, {"name": "condicion_economica", "dataType": "String"}, {"name": "tarjeta_transaccional_mes", "dataType": "Boolean"}, {"name": "codigo_producto", "dataType": "String"}, {"name": "bloqueo_duro_tarjeta", "dataType": "Boolean"}, {"name": "cantidad_compras_totales_3meses", "dataType": "Bigint"}, {"name": "codigo_destino", "dataType": "String"}, {"name": "descripcion_crm", "dataType": "String"}, {"name": "sub_tipo_producto_comercial", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "pan", "dataType": "String"}, {"name": "idf_pers_ods_tarjeta", "dataType": "String"}, {"name": "motivo_baja_contrato", "dataType": "String"}, {"name": "numero_contrato", "dataType": "String"}, {"name": "es_titular", "dataType": "Boolean"}, {"name": "bloqueo_soft_tarjeta", "dataType": "Boolean"}, {"name": "idf_pers_ods_contrato", "dataType": "String"}, {"name": "bloqueo_duro_contrato", "dataType": "Boolean"}, {"name": "cantidad_compras_totales", "dataType": "Integer"}, {"name": "fecha_baja_contrato", "dataType": "Date"}, {"name": "codigo_subproducto", "dataType": "String"}, {"name": "codigo_bloqueo_contrato", "dataType": "Double"}, {"name": "tipo_tarjeta", "dataType": "String"}, {"name": "fecha_alta_contrato", "dataType": "Date"}, {"name": "limite_tarjeta", "dataType": "Decimal"}, {"name": "bloqueo_soft_contrato", "dataType": "Boolean"}, {"name": "condicion_economica", "dataType": "String"}, {"name": "tarjeta_transaccional_mes", "dataType": "Boolean"}, {"name": "codigo_producto", "dataType": "String"}, {"name": "bloqueo_duro_tarjeta", "dataType": "Boolean"}, {"name": "cantidad_compras_totales_3meses", "dataType": "Bigint"}, {"name": "codigo_destino", "dataType": "String"}, {"name": "descripcion_crm", "dataType": "String"}, {"name": "sub_tipo_producto_comercial", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_1011_0 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT 
    CAST(1 AS INTEGER) AS CodProducto,
    * EXCEPT (`codproducto`)
  
  FROM Union_1010 AS in0

),

AlteryxSelect_1012 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT 
    idf_pers_ods_tarjeta AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6,
    Col7 AS Col7,
    Col8 AS Col8,
    Col9 AS Col9
  
  FROM Formula_1011_0 AS in0

)

SELECT *

FROM AlteryxSelect_1012
