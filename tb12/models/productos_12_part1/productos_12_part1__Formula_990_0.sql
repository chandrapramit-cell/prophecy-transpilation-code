{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Filter_989 AS (

  SELECT *
  
  FROM {{ ref('productos_12_part1__Filter_989')}}

),

Formula_990_0 AS (

  {#VisualGroup: CalculoTCCodigoProducto1436373839#}
  SELECT 
    CAST(14 AS INTEGER) AS CodProducto,
    CAST(descripcion_crm AS string) AS Col1,
    CAST((
      CONCAT(
        'Fecha Alta: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST((DAY(fecha_alta_contrato)) AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            '')
        ), 
        '-', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(EXTRACT(MONTH FROM fecha_alta_contrato) AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            '')
        ), 
        '-', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(EXTRACT(YEAR FROM fecha_alta_contrato) AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            '')
        ))
    ) AS string) AS Col2,
    CAST((
      CONCAT(
        'Limite: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(limite_tarjeta AS DOUBLE), 2)), ',', '__THS__')), '__THS__', ',')))
    ) AS string) AS Col3,
    CAST((
      CONCAT(
        'Tipo tarjeta: ', 
        (
          CASE
            WHEN (es_titular = TRUE)
              THEN 'Titular'
            ELSE 'Adicional'
          END
        ))
    ) AS string) AS Col4,
    CAST((CONCAT('Pan: ', pan)) AS string) AS Col5,
    CAST((CONCAT('Cuenta: ', numero_contrato)) AS string) AS Col6,
    CAST((
      CONCAT(
        'Estado: ', 
        (
          CASE
            WHEN (
              (
                ((bloqueo_soft_contrato = FALSE) AND (bloqueo_duro_contrato = FALSE))
                AND (bloqueo_soft_tarjeta = FALSE)
              )
              AND (bloqueo_duro_tarjeta = FALSE)
            )
              THEN ' Ok'
            ELSE ' Bloqueado'
          END
        ))
    ) AS string) AS Col7,
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
    ) AS string) AS Col8,
    CAST((
      CONCAT(
        'Cantidad transacciones 3 mes: ', 
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
    ) AS string) AS Col9,
    *
  
  FROM Filter_989 AS in0

)

SELECT *

FROM Formula_990_0
