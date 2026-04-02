{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_AlteryxSelect_1025",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Join_988_inner AS (

  SELECT *
  
  FROM {{ ref('productos_part1__Join_988_inner')}}

),

Filter_1017_to_Filter_1019 AS (

  {#VisualGroup: TarjetasCorporativasCodigoProducto21#}
  SELECT * 
  
  FROM Join_988_inner AS in0
  
  WHERE (
          (
            (
              contains(coalesce(lower(sub_tipo_producto_comercial), ''), lower('Corporativa'))
              OR contains(coalesce(lower(sub_tipo_producto_comercial), ''), lower('Pyme'))
            )
            AND (bloqueo_duro_contrato = false)
          )
          AND (bloqueo_duro_tarjeta = false)
        )

),

Summarize_1018 AS (

  {#VisualGroup: TarjetasCorporativasCodigoProducto21#}
  SELECT 
    COUNT(DISTINCT pan) AS cantidad_tarjetas,
    SUM(cantidad_compras_totales) AS cantidad_compras_totales,
    SUM(cantidad_compras_totales_3meses) AS cantidad_compras_totales_3meses,
    SUM(limite_tarjeta) AS limite_tarjeta,
    idf_pers_ods_contrato AS idf_pers_ods_contrato,
    numero_contrato AS numero_contrato,
    descripcion_crm AS descripcion_crm
  
  FROM Filter_1017_to_Filter_1019 AS in0
  
  GROUP BY 
    idf_pers_ods_contrato, numero_contrato, descripcion_crm

),

Summarize_1022 AS (

  {#VisualGroup: TarjetasCorporativasCodigoProducto21#}
  SELECT 
    COUNT(DISTINCT pan) AS cantidad_tarjetas,
    idf_pers_ods_contrato AS idf_pers_ods_contrato,
    numero_contrato AS numero_contrato,
    descripcion_crm AS descripcion_crm
  
  FROM Filter_1017_to_Filter_1019 AS in0
  
  GROUP BY 
    idf_pers_ods_contrato, numero_contrato, descripcion_crm

),

Join_1023_left_UnionLeftOuter AS (

  {#VisualGroup: TarjetasCorporativasCodigoProducto21#}
  SELECT 
    (
      CASE
        WHEN (
          (in0.idf_pers_ods_contrato = in1.idf_pers_ods_contrato)
          AND (in0.numero_contrato = in1.numero_contrato)
        )
          THEN in1.cantidad_tarjetas
        ELSE NULL
      END
    ) AS cantidad_tarjetas_con_consumo,
    in0.*,
    in1.* EXCEPT (`idf_pers_ods_contrato`, `numero_contrato`, `descripcion_crm`, `cantidad_tarjetas`)
  
  FROM Summarize_1018 AS in0
  LEFT JOIN Summarize_1022 AS in1
     ON (
      (in0.idf_pers_ods_contrato = in1.idf_pers_ods_contrato)
      AND (in0.numero_contrato = in1.numero_contrato)
    )

),

Formula_1020_0 AS (

  {#VisualGroup: TarjetasCorporativasCodigoProducto21#}
  SELECT 
    CAST((
      CASE
        WHEN ((cantidad_tarjetas_con_consumo IS NULL) OR ((LENGTH(cantidad_tarjetas_con_consumo)) = 0))
          THEN CAST(0 AS string)
        ELSE cantidad_tarjetas_con_consumo
      END
    ) AS INTEGER) AS cantidad_tarjetas_con_consumo,
    CAST(21 AS INTEGER) AS CodProducto,
    CAST(descripcion_crm AS string) AS Col1,
    CAST((
      CONCAT(
        'Cantidad de Tarjetas: ', 
        (
          CASE
            WHEN ((cantidad_tarjetas IS NULL) OR ((LENGTH(CAST(cantidad_tarjetas AS string))) = 0))
              THEN 0
            ELSE (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_tarjetas AS DOUBLE), 0)), ',', '__THS__')), '__THS__', ''))
          END
        ))
    ) AS string) AS Col2,
    * EXCEPT (`cantidad_tarjetas_con_consumo`)
  
  FROM Join_1023_left_UnionLeftOuter AS in0

),

Formula_1020_1 AS (

  {#VisualGroup: TarjetasCorporativasCodigoProducto21#}
  SELECT 
    CAST((
      CONCAT(
        'Cantidad de Tarjetas con Consumo: ', 
        (
          CASE
            WHEN (
              (cantidad_tarjetas_con_consumo IS NULL)
              OR ((LENGTH(CAST(cantidad_tarjetas_con_consumo AS string))) = 0)
            )
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_tarjetas_con_consumo AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS string) AS Col3,
    CAST((
      CONCAT(
        'Cantidad transacciones mes: ', 
        (
          CASE
            WHEN ((cantidad_compras_totales IS NULL) OR ((LENGTH(CAST(cantidad_compras_totales AS string))) = 0))
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_compras_totales AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS string) AS Col4,
    CAST((
      CONCAT(
        'Cantidad transacciones 3 meses: ', 
        (
          CASE
            WHEN (
              (cantidad_compras_totales_3meses IS NULL)
              OR ((LENGTH(CAST(cantidad_compras_totales_3meses AS string))) = 0)
            )
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_compras_totales_3meses AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS string) AS Col5,
    *
  
  FROM Formula_1020_0 AS in0

),

AlteryxSelect_1025 AS (

  {#VisualGroup: TarjetasCorporativasCodigoProducto21#}
  SELECT 
    idf_pers_ods_contrato AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5
  
  FROM Formula_1020_1 AS in0

)

SELECT *

FROM AlteryxSelect_1025
