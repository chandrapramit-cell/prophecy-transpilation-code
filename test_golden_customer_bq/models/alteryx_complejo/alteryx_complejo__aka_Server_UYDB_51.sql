{{
  config({    
    "materialized": "table",
    "alias": "aka_Server_UYDB_51_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_Server_UYDB_502 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_502_ref') }}

),

Unique_505 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_502 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY codigo_producto, codigo_subproducto ORDER BY codigo_producto, codigo_subproducto) = 1

),

aka_Server_UYDB_504 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_504_ref') }}

),

aka_Server_UYDB_501 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_501_ref') }}

),

Summarize_503 AS (

  SELECT 
    SUM(cantidad_compras_totales) AS cantidad_compras_totales_3meses,
    idf_pers_ods_tarjeta AS idf_pers_ods_tarjeta,
    numero_contrato AS numero_contrato,
    pan_hash AS pan_hash
  
  FROM aka_Server_UYDB_501 AS in0
  
  GROUP BY 
    idf_pers_ods_tarjeta, numero_contrato, pan_hash

),

Join_529_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods_tarjeta`, `numero_contrato`, `pan_hash`)
  
  FROM aka_Server_UYDB_504 AS in0
  LEFT JOIN Summarize_503 AS in1
     ON (
      (
        (in0.idf_pers_ods_tarjeta = in1.idf_pers_ods_tarjeta)
        AND (in0.numero_contrato = in1.numero_contrato)
      )
      AND (in0.pan_hash = in1.pan_hash)
    )

),

aka_Server_UYDB_827 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_827_ref') }}

),

Join_828_inner AS (

  SELECT 
    in1.pan_completo AS pan,
    in0.* EXCEPT (`pan_hash`)
  
  FROM Join_529_left_UnionLeftOuter AS in0
  INNER JOIN aka_Server_UYDB_827 AS in1
     ON ((in0.numero_contrato = in1.numero_contrato) AND (in0.pan_hash = in1.pan_hash))

),

Cleanse_531 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_828_inner'], 
      [
        { "name": "pan", "dataType": "String" }, 
        { "name": "idf_pers_ods_tarjeta", "dataType": "String" }, 
        { "name": "motivo_baja_contrato", "dataType": "String" }, 
        { "name": "numero_contrato", "dataType": "String" }, 
        { "name": "es_titular", "dataType": "Boolean" }, 
        { "name": "bloqueo_soft_tarjeta", "dataType": "Boolean" }, 
        { "name": "idf_pers_ods_contrato", "dataType": "String" }, 
        { "name": "bloqueo_duro_contrato", "dataType": "Boolean" }, 
        { "name": "cantidad_compras_totales", "dataType": "Numeric" }, 
        { "name": "fecha_baja_contrato", "dataType": "Date" }, 
        { "name": "codigo_subproducto", "dataType": "String" }, 
        { "name": "codigo_bloqueo_contrato", "dataType": "Float" }, 
        { "name": "tipo_tarjeta", "dataType": "String" }, 
        { "name": "fecha_alta_contrato", "dataType": "Date" }, 
        { "name": "limite_tarjeta", "dataType": "String" }, 
        { "name": "bloqueo_soft_contrato", "dataType": "Boolean" }, 
        { "name": "condicion_economica", "dataType": "String" }, 
        { "name": "tarjeta_transaccional_mes", "dataType": "Boolean" }, 
        { "name": "codigo_producto", "dataType": "String" }, 
        { "name": "bloqueo_duro_tarjeta", "dataType": "Boolean" }, 
        { "name": "cantidad_compras_totales_3meses", "dataType": "Numeric" }
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

Join_506_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`codigo_producto`, 
    `codigo_subproducto`, 
    `familia_producto_comercial`, 
    `grupo_producto_comercial`, 
    `tipo_producto_comercial`)
  
  FROM Cleanse_531 AS in0
  INNER JOIN Unique_505 AS in1
     ON ((in0.codigo_producto = in1.codigo_producto) AND (in0.codigo_subproducto = in1.codigo_subproducto))

),

Filter_518 AS (

  SELECT * 
  
  FROM Join_506_inner AS in0
  
  WHERE (
          (codigo_producto = '88')
          AND (
                ((CAST(codigo_subproducto AS STRING) = '5601') OR (CAST(codigo_subproducto AS STRING) = '7601'))
                OR (CAST(codigo_subproducto AS STRING) = '7603')
              )
        )

),

Summarize_515 AS (

  SELECT DISTINCT idf_pers_ods_tarjeta AS idf_pers_ods
  
  FROM Filter_518 AS in0

),

aka_Server_UYDB_704 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_704_ref') }}

),

Formula_725_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((STRPOS((coalesce(LOWER(datos_adicionales), '')), LOWER('BPS'))) > 0)
          THEN 'BPS  '
        WHEN ((STRPOS((coalesce(LOWER(datos_adicionales), '')), LOWER('DGI'))) > 0)
          THEN 'DGI  '
        ELSE 'Otros'
      END
    ) AS STRING) AS TipoPago,
    *
  
  FROM aka_Server_UYDB_704 AS in0

),

aka_Server_UYDB_311 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_311_ref') }}

),

Filter_732 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_311 AS in0
  
  WHERE (perimetro_gestion = CAST('1' AS FLOAT64))

),

Unique_643 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_502 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY codigo_producto, codigo_subproducto, codigo_destino ORDER BY codigo_producto, codigo_subproducto, codigo_destino) = 1

),

Join_639_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`codigo_producto`, `codigo_subproducto`, `codigo_destino`)
  
  FROM Filter_732 AS in0
  INNER JOIN Unique_643 AS in1
     ON (
      ((in0.codigo_producto = in1.codigo_producto) AND (in0.codigo_subproducto = in1.codigo_subproducto))
      AND (in0.codigo_destino = in1.codigo_destino)
    )

),

Filter_659_reject_to_Filter_820 AS (

  SELECT * 
  
  FROM Join_639_inner AS in0
  
  WHERE (
          (
            ((tipo_producto_comercial <> 'Coche') OR ((tipo_producto_comercial = 'Coche') IS NULL))
            AND (
                  (
                    (NOT CAST(((STRPOS(tipo_producto_comercial, 'GTM')) > 0) AS BOOL))
                    OR (CAST(((STRPOS(tipo_producto_comercial, 'GTM')) > 0) AS BOOL) IS NULL)
                  )
                  AND (
                        (NOT CAST(((STRPOS(tipo_producto_comercial, 'Leasing')) > 0) AS BOOL))
                        OR (CAST(((STRPOS(tipo_producto_comercial, 'Leasing')) > 0) AS BOOL) IS NULL)
                      )
                )
          )
          AND (
                (
                  (grupo_producto_comercial <> 'HIpotecarios')
                  OR ((grupo_producto_comercial = 'HIpotecarios') IS NULL)
                )
                AND (
                      ((grupo_producto_comercial <> 'Consumo') OR ((grupo_producto_comercial = 'Consumo') IS NULL))
                      AND (grupo_producto_comercial = 'Comerciales')
                    )
              )
        )

),

Formula_302_0 AS (

  SELECT 
    32 AS CodProducto,
    CAST((
      CONCAT(
        'Destino: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(codigo_destino AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ' ', 
        descripcion_crm)
    ) AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Monto: ', 
        divisa, 
        ' ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(saldo_moneda_origen AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT(
        'Op.: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(numero_contrato AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Col3,
    *
  
  FROM Filter_659_reject_to_Filter_820 AS in0

),

aka_Server_UYDB_551 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_551_ref') }}

),

Filter_885 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_551 AS in0
  
  WHERE (banco_cobrador = CAST('137' AS INT64))

),

Summarize_854 AS (

  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM Filter_885 AS in0

),

Sort_855 AS (

  SELECT * 
  
  FROM Summarize_854 AS in0
  
  ORDER BY AAAAMM DESC

),

Sample_856 AS (

  {{ prophecy_basics.Sample('Sort_855', [], 1002, 'firstN', 17) }}

),

RecordID_857 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `MesId`
  
  FROM Sample_856

),

Join_852_inner AS (

  SELECT 
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods_pagador AS idf_pers_ods_pagador,
    in0.importe_arbitrado_dolares AS importe_arbitrado_dolares,
    in0.idf_pers_ods_cobrador AS idf_pers_ods_cobrador,
    in1.MesId AS MesId
  
  FROM Filter_885 AS in0
  INNER JOIN RecordID_857 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_858 AS (

  SELECT DISTINCT idf_pers_ods_pagador AS idf_pers_ods
  
  FROM Join_852_inner AS in0

),

aka_Server_UYDB_616 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_616_ref') }}

),

Summarize_617 AS (

  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM aka_Server_UYDB_616 AS in0

),

Sort_619 AS (

  SELECT * 
  
  FROM Summarize_617 AS in0
  
  ORDER BY AAAAMM DESC

),

Filter_507 AS (

  SELECT * 
  
  FROM Join_506_inner AS in0
  
  WHERE (tipo_tarjeta = 'TC')

),

Summarize_560 AS (

  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM aka_Server_UYDB_551 AS in0

),

Sort_561 AS (

  SELECT * 
  
  FROM Summarize_560 AS in0
  
  ORDER BY AAAAMM DESC

),

Sample_562 AS (

  {{ prophecy_basics.Sample('Sort_561', [], 1002, 'firstN', 17) }}

),

RecordID_563 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `MesId`
  
  FROM Sample_562

),

Filter_885_reject AS (

  SELECT * 
  
  FROM aka_Server_UYDB_551 AS in0
  
  WHERE ((banco_cobrador <> CAST('137' AS INT64)) OR ((banco_cobrador = CAST('137' AS INT64)) IS NULL))

),

Summarize_873 AS (

  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM Filter_885_reject AS in0

),

Sort_874 AS (

  SELECT * 
  
  FROM Summarize_873 AS in0
  
  ORDER BY AAAAMM DESC

),

Summarize_713 AS (

  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM Formula_725_0 AS in0

),

aka_Server_UYDB_543 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_543_ref') }}

),

Filter_544_reject AS (

  SELECT * 
  
  FROM aka_Server_UYDB_543 AS in0
  
  WHERE (
          (NOT((grupo_producto = 'Vida') AND (tipo_producto = 'Vida Empresarial')))
          OR (((grupo_producto = 'Vida') AND (tipo_producto = 'Vida Empresarial')) IS NULL)
        )

),

Formula_546_0 AS (

  SELECT 
    CAST((
      CONCAT(
        grupo_producto, 
        (
          CASE
            WHEN (((grupo_producto IS NULL) OR (tipo_producto IS NULL)) OR (grupo_producto <> tipo_producto))
              THEN CAST((CONCAT(' - ', tipo_producto)) AS STRING)
            ELSE ''
          END
        ))
    ) AS STRING) AS Col1,
    CAST(sub_tipo_producto AS STRING) AS Col2,
    CAST((CONCAT('Plan: ', TRIM(codigo_plan))) AS STRING) AS Col3,
    CAST((CONCAT('Desde Fecha: ', (FORMAT_TIMESTAMP('%d-%m-%Y', fecha_alta)))) AS STRING) AS Col4,
    (
      CASE
        WHEN (
          (
            (grupo_producto = 'Accidentes Personales')
            OR ((grupo_producto = 'Vida') AND (tipo_producto = 'Vida'))
          )
          OR ((grupo_producto = 'Robo Y Otros') AND (tipo_producto = 'Viaje'))
        )
          THEN 16
        WHEN (grupo_producto = 'Coche')
          THEN 11
        WHEN (grupo_producto = 'Comercio A Primer Riesgo')
          THEN 30
        WHEN (CAST(grupo_producto AS STRING) IN ('Garantia Alquiler', 'Incendio', 'Vivienda'))
          THEN 10
        WHEN ((grupo_producto = 'Robo Y Otros') AND (tipo_producto = 'Fraude Full'))
          THEN 6
        ELSE 99
      END
    ) AS CodProducto,
    *
  
  FROM Filter_544_reject AS in0

),

Filter_549 AS (

  SELECT * 
  
  FROM Formula_546_0 AS in0
  
  WHERE (CodProducto <> CAST('99' AS INT64))

),

AlteryxSelect_548 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4
  
  FROM Filter_549 AS in0

),

Union_49_reformat_13 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4
  
  FROM AlteryxSelect_548 AS in0

),

aka_Server_UYDB_662 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_662_ref') }}

),

Filter_663 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_662 AS in0
  
  WHERE (
          (
            ((motivo_cancelacion IS NULL) OR ((LENGTH(motivo_cancelacion)) = 0))
            AND ((fecha_cancelacion IS NULL) OR ((LENGTH(CAST(fecha_cancelacion AS STRING))) = 0))
          )
          AND ((fecha_cierre IS NULL) OR ((LENGTH(CAST(fecha_cierre AS STRING))) = 0))
        )

),

Summarize_664 AS (

  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Filter_663 AS in0

),

aka_Server_UYDB_692 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_692_ref') }}

),

Summarize_681 AS (

  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM aka_Server_UYDB_692 AS in0

),

Sort_682 AS (

  SELECT * 
  
  FROM Summarize_681 AS in0
  
  ORDER BY AAAAMM DESC

),

Sample_683 AS (

  {{ prophecy_basics.Sample('Sort_682', [], 1002, 'firstN', 17) }}

),

RecordID_684 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `MesId`
  
  FROM Sample_683

),

Join_680_inner AS (

  SELECT 
    in0.monto_arbitrado_dolares AS monto_arbitrado_dolares,
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods AS idf_pers_ods,
    in0.merchant AS merchant,
    in1.MesId AS MesId
  
  FROM aka_Server_UYDB_692 AS in0
  INNER JOIN RecordID_684 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_685 AS (

  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Join_680_inner AS in0

),

Summarize_690 AS (

  SELECT DISTINCT merchant AS merchant
  
  FROM aka_Server_UYDB_692 AS in0

),

AppendFields_688 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_690 AS in0
  INNER JOIN RecordID_684 AS in1
     ON true

),

AppendFields_686 AS (

  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.AAAAMM AS AAAAMM,
    in0.MesId AS MesId,
    in0.merchant AS merchant
  
  FROM AppendFields_688 AS in0
  INNER JOIN Summarize_685 AS in1
     ON true

),

Filter_544 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_543 AS in0
  
  WHERE ((grupo_producto = 'Vida') AND (tipo_producto = 'Vida Empresarial'))

),

aka_Server_UYDB_696 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_696_ref') }}

),

Summarize_697 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((idf_pers_ods IS NULL) OR (CAST(idf_pers_ods AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS cantidad_operaciones,
    SUM(saldo_arbitrado_dolares) AS saldo_arbitrado_dolares,
    idf_pers_ods AS idf_pers_ods
  
  FROM aka_Server_UYDB_696 AS in0
  
  GROUP BY idf_pers_ods

),

Formula_841_0 AS (

  SELECT 
    (
      CASE
        WHEN (
          (codigo_producto = '77')
          AND ((STRPOS((coalesce(LOWER(UPPER(descripcion_crm)), '')), LOWER('SOY'))) > 0)
        )
          THEN 37
        WHEN (
          (codigo_producto = '77')
          AND ((STRPOS((coalesce(LOWER(UPPER(descripcion_crm)), '')), LOWER('SOY'))) <= 0)
        )
          THEN 36
        WHEN (
          (codigo_producto = '80')
          AND ((STRPOS((coalesce(LOWER(UPPER(descripcion_crm)), '')), LOWER('SOY'))) > 0)
        )
          THEN 39
        WHEN (
          (codigo_producto = '80')
          AND ((STRPOS((coalesce(LOWER(UPPER(descripcion_crm)), '')), LOWER('SOY'))) <= 0)
        )
          THEN 38
        ELSE 99
      END
    ) AS CodProducto,
    *
  
  FROM Filter_507 AS in0

),

AlteryxSelect_303 AS (

  SELECT 
    CAST(idf_pers_ods AS STRING) AS IDF_PERS_ODS,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3
  
  FROM Formula_302_0 AS in0

),

Union_49_reformat_6 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM AlteryxSelect_303 AS in0

),

aka_Server_UYDB_660 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_660_ref') }}

),

Summarize_661 AS (

  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM aka_Server_UYDB_660 AS in0

),

Formula_350_0 AS (

  SELECT 
    23 AS CodProducto,
    *
  
  FROM Summarize_661 AS in0

),

Union_49_reformat_26 AS (

  SELECT CAST(CodProducto AS STRING) AS CodProducto
  
  FROM Formula_350_0 AS in0

),

Sample_620 AS (

  {{ prophecy_basics.Sample('Sort_619', [], 1002, 'firstN', 18) }}

),

RecordID_622 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `MesID`
  
  FROM Sample_620

),

Formula_624_0 AS (

  SELECT 
    'Fechas' AS Fechas,
    *
  
  FROM RecordID_622 AS in0

),

CrossTab_623 AS (

  SELECT *
  
  FROM (
    SELECT 
      MesID,
      AAAAMM
    
    FROM Formula_624_0 AS in0
  )
  PIVOT (
    FIRST(AAAAMM) AS First
    FOR MesID
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '1', '17', '14', '2', '18', '7', '3'
    )
  )

),

Join_636_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`AAAAMM`)
  
  FROM RecordID_622 AS in0
  INNER JOIN aka_Server_UYDB_616 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_618 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((idf_pers_ods_cobrador IS NULL) OR (CAST(idf_pers_ods_cobrador AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS cantidad_pagos,
    COUNT(DISTINCT idf_pers_ods_cobrador) AS cantidad_empresas_cobradoras,
    SUM(importe_arbitrado_dolares) AS importe_arbitrado_dolares,
    idf_pers_ods_pagador AS idf_pers_ods,
    MesID AS MesID
  
  FROM Join_636_inner AS in0
  
  GROUP BY 
    idf_pers_ods_pagador, MesID

),

CrossTab_632 AS (

  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesID,
      IMPORTE_ARBITRADO_DOLARES
    
    FROM Summarize_618 AS in0
  )
  PIVOT (
    SUM(IMPORTE_ARBITRADO_DOLARES) AS Sum
    FOR MesID
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '1', '17', '14', '2', '18', '7', '3'
    )
  )

),

AppendFields_629 AS (

  SELECT 
    in0.`1` AS Source_1,
    in0.`2` AS Source_2,
    in0.`3` AS Source_3,
    in0.`4` AS Source_4,
    in0.`5` AS Source_5,
    in0.`6` AS Source_6,
    in0.`7` AS Source_7,
    in0.`8` AS Source_8,
    in0.`9` AS Source_9,
    in0.`10` AS Source_10,
    in0.`11` AS Source_11,
    in0.`12` AS Source_12,
    in0.`13` AS Source_13,
    in0.`14` AS Source_14,
    in0.`15` AS Source_15,
    in0.`16` AS Source_16,
    in0.`17` AS Source_17,
    in1.*
  
  FROM CrossTab_623 AS in0
  INNER JOIN CrossTab_632 AS in1
     ON true

),

Cleanse_630 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AppendFields_629'], 
      [
        { "name": "Source_5", "dataType": "String" }, 
        { "name": "12", "dataType": "Double" }, 
        { "name": "8", "dataType": "Double" }, 
        { "name": "4", "dataType": "Double" }, 
        { "name": "Source_11", "dataType": "String" }, 
        { "name": "15", "dataType": "Double" }, 
        { "name": "11", "dataType": "Double" }, 
        { "name": "9", "dataType": "Double" }, 
        { "name": "Source_13", "dataType": "String" }, 
        { "name": "Source_9", "dataType": "String" }, 
        { "name": "Source_17", "dataType": "String" }, 
        { "name": "13", "dataType": "Double" }, 
        { "name": "Source_1", "dataType": "String" }, 
        { "name": "16", "dataType": "Double" }, 
        { "name": "5", "dataType": "Double" }, 
        { "name": "10", "dataType": "Double" }, 
        { "name": "Source_3", "dataType": "String" }, 
        { "name": "Source_7", "dataType": "String" }, 
        { "name": "6", "dataType": "Double" }, 
        { "name": "Source_16", "dataType": "String" }, 
        { "name": "1", "dataType": "Double" }, 
        { "name": "17", "dataType": "Double" }, 
        { "name": "Source_14", "dataType": "String" }, 
        { "name": "14", "dataType": "Double" }, 
        { "name": "Source_2", "dataType": "String" }, 
        { "name": "Source_4", "dataType": "String" }, 
        { "name": "2", "dataType": "Double" }, 
        { "name": "idf_pers_ods", "dataType": "String" }, 
        { "name": "Source_8", "dataType": "String" }, 
        { "name": "Source_6", "dataType": "String" }, 
        { "name": "18", "dataType": "String" }, 
        { "name": "7", "dataType": "Double" }, 
        { "name": "Source_15", "dataType": "String" }, 
        { "name": "3", "dataType": "Double" }, 
        { "name": "Source_10", "dataType": "String" }, 
        { "name": "Source_12", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13'], 
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

Formula_631_0 AS (

  SELECT 
    'Volumen Arbitrado USD' AS Col1,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_1 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `1`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_2 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `2`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col3,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_3 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `3`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col4,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_4 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `4`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col5,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_5 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `5`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col6,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_6 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `6`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col7,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_7 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `7`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col8,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_8 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `8`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col9,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_9 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `9`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col10,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_10 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `10`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col11,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_11 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `11`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col12,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_12 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', `12`)), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col13,
    22 AS CodProducto,
    *
  
  FROM Cleanse_630 AS in0

),

AlteryxSelect_628 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6,
    Col7 AS Col7,
    Col8 AS Col8,
    Col9 AS Col9,
    Col10 AS Col10,
    Col11 AS Col11,
    Col12 AS Col12,
    Col13 AS Col13,
    CodProducto AS CodProducto
  
  FROM Formula_631_0 AS in0

),

CrossTab_621 AS (

  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesID,
      CANTIDAD_PAGOS
    
    FROM Summarize_618 AS in0
  )
  PIVOT (
    SUM(CANTIDAD_PAGOS) AS Sum
    FOR MesID
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '1', '17', '14', '2', '18', '7', '3'
    )
  )

),

AppendFields_626 AS (

  SELECT 
    in0.`1` AS Source_1,
    in0.`2` AS Source_2,
    in0.`3` AS Source_3,
    in0.`4` AS Source_4,
    in0.`5` AS Source_5,
    in0.`6` AS Source_6,
    in0.`7` AS Source_7,
    in0.`8` AS Source_8,
    in0.`9` AS Source_9,
    in0.`10` AS Source_10,
    in0.`11` AS Source_11,
    in0.`12` AS Source_12,
    in0.`13` AS Source_13,
    in0.`14` AS Source_14,
    in0.`15` AS Source_15,
    in0.`16` AS Source_16,
    in0.`17` AS Source_17,
    in1.*
  
  FROM CrossTab_623 AS in0
  INNER JOIN CrossTab_621 AS in1
     ON true

),

Cleanse_627 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AppendFields_626'], 
      [
        { "name": "Source_5", "dataType": "String" }, 
        { "name": "12", "dataType": "Integer" }, 
        { "name": "8", "dataType": "Integer" }, 
        { "name": "4", "dataType": "Integer" }, 
        { "name": "Source_11", "dataType": "String" }, 
        { "name": "15", "dataType": "Integer" }, 
        { "name": "11", "dataType": "Integer" }, 
        { "name": "9", "dataType": "Integer" }, 
        { "name": "Source_13", "dataType": "String" }, 
        { "name": "Source_9", "dataType": "String" }, 
        { "name": "Source_17", "dataType": "String" }, 
        { "name": "13", "dataType": "Integer" }, 
        { "name": "Source_1", "dataType": "String" }, 
        { "name": "16", "dataType": "Integer" }, 
        { "name": "5", "dataType": "Integer" }, 
        { "name": "10", "dataType": "Integer" }, 
        { "name": "Source_3", "dataType": "String" }, 
        { "name": "Source_7", "dataType": "String" }, 
        { "name": "6", "dataType": "Integer" }, 
        { "name": "Source_16", "dataType": "String" }, 
        { "name": "1", "dataType": "Integer" }, 
        { "name": "17", "dataType": "Integer" }, 
        { "name": "Source_14", "dataType": "String" }, 
        { "name": "14", "dataType": "Integer" }, 
        { "name": "Source_2", "dataType": "String" }, 
        { "name": "Source_4", "dataType": "String" }, 
        { "name": "2", "dataType": "Integer" }, 
        { "name": "idf_pers_ods", "dataType": "String" }, 
        { "name": "Source_8", "dataType": "String" }, 
        { "name": "Source_6", "dataType": "String" }, 
        { "name": "18", "dataType": "String" }, 
        { "name": "7", "dataType": "Integer" }, 
        { "name": "Source_15", "dataType": "String" }, 
        { "name": "3", "dataType": "Integer" }, 
        { "name": "Source_10", "dataType": "String" }, 
        { "name": "Source_12", "dataType": "String" }
      ], 
      'keepOriginal', 
      [], 
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

Formula_635_0 AS (

  SELECT 
    'Cantidad de Pagos' AS Col1,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_1 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`1` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_2 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`2` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col3,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_3 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`3` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col4,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_4 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`4` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col5,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_5 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`5` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col6,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_6 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`6` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col7,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_7 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`7` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col8,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_8 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`8` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col9,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_9 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`9` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col10,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_10 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`10` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col11,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_11 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`11` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col12,
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Source_12 AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`12` AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col13,
    22 AS CodProducto,
    *
  
  FROM Cleanse_627 AS in0

),

AlteryxSelect_625 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6,
    Col7 AS Col7,
    Col8 AS Col8,
    Col9 AS Col9,
    Col10 AS Col10,
    Col11 AS Col11,
    Col12 AS Col12,
    Col13 AS Col13,
    CodProducto AS CodProducto
  
  FROM Formula_635_0 AS in0

),

Union_633 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_625', 'AlteryxSelect_628'], 
      [
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_634 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6,
    Col7 AS Col7,
    Col8 AS Col8,
    Col9 AS Col9,
    Col10 AS Col10,
    Col11 AS Col11,
    Col12 AS Col12,
    Col13 AS Col13
  
  FROM Union_633 AS in0

),

Union_49_reformat_15 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col10 AS STRING) AS Col10,
    CAST(Col11 AS STRING) AS Col11,
    CAST(Col12 AS STRING) AS Col12,
    CAST(Col13 AS STRING) AS Col13,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5,
    CAST(Col6 AS STRING) AS Col6,
    CAST(Col7 AS STRING) AS Col7,
    CAST(Col8 AS STRING) AS Col8,
    CAST(Col9 AS STRING) AS Col9
  
  FROM AlteryxSelect_634 AS in0

),

aka_Server_UYDB_606 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_606_ref') }}

),

Join_829_inner AS (

  SELECT 
    in1.pan_completo AS pan,
    in0.* EXCEPT (`pan_hash`)
  
  FROM aka_Server_UYDB_606 AS in0
  INNER JOIN aka_Server_UYDB_827 AS in1
     ON (in0.pan_hash = in1.pan_hash)

),

Filter_607 AS (

  SELECT * 
  
  FROM Join_829_inner AS in0
  
  WHERE (tipo_transaccion = 'Compra TC')

),

Summarize_609 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((monto_arbitrado_dolares IS NULL) OR (CAST(monto_arbitrado_dolares AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS cantidad_de_transacciones,
    SUM(monto_arbitrado_dolares) AS monto_arbitrado_dolares,
    AAAAMM AS AAAAMM,
    idf_pers_ods AS idf_pers_ods,
    pan AS pan
  
  FROM Filter_607 AS in0
  
  GROUP BY 
    AAAAMM, idf_pers_ods, pan

),

Formula_608_0 AS (

  SELECT 
    4 AS CodProducto,
    CAST((
      CONCAT(
        'PAN: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(pan AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Mes: ', 
        (
          SUBSTRING(
            CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(AAAAMM AS FLOAT64))), ',', '__THS__')), '__THS__', '')) AS STRING), 
            5, 
            2)
        ), 
        '-', 
        (
          SUBSTRING(
            CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(AAAAMM AS FLOAT64))), ',', '__THS__')), '__THS__', '')) AS STRING), 
            1, 
            4)
        ))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT(
        'Cantidad Transacciones: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', cantidad_de_transacciones)), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Col3,
    CAST((
      CONCAT(
        'Volumen de Transacciones: USD ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.2f', CAST(monto_arbitrado_dolares AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col4,
    *
  
  FROM Summarize_609 AS in0

),

AlteryxSelect_610 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4
  
  FROM Formula_608_0 AS in0

),

Union_49_reformat_10 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4
  
  FROM AlteryxSelect_610 AS in0

),

Filter_532_to_Filter_534 AS (

  SELECT * 
  
  FROM Join_506_inner AS in0
  
  WHERE (
          (
            (
              CAST(((STRPOS((coalesce(LOWER(sub_tipo_producto_comercial), '')), LOWER('Corporativa'))) > 0) AS BOOL)
              OR CAST(((STRPOS((coalesce(LOWER(sub_tipo_producto_comercial), '')), LOWER('Pyme'))) > 0) AS BOOL)
            )
            AND (bloqueo_duro_contrato = FALSE)
          )
          AND (bloqueo_duro_tarjeta = FALSE)
        )

),

Summarize_537 AS (

  SELECT 
    COUNT(DISTINCT pan) AS cantidad_tarjetas,
    idf_pers_ods_contrato AS idf_pers_ods_contrato,
    numero_contrato AS numero_contrato,
    descripcion_crm AS descripcion_crm
  
  FROM Filter_532_to_Filter_534 AS in0
  
  GROUP BY 
    idf_pers_ods_contrato, numero_contrato, descripcion_crm

),

Summarize_533 AS (

  SELECT 
    COUNT(DISTINCT pan) AS cantidad_tarjetas,
    SUM(cantidad_compras_totales) AS cantidad_compras_totales,
    SUM(cantidad_compras_totales_3meses) AS cantidad_compras_totales_3meses,
    SUM(limite_tarjeta) AS limite_tarjeta,
    idf_pers_ods_contrato AS idf_pers_ods_contrato,
    numero_contrato AS numero_contrato,
    descripcion_crm AS descripcion_crm
  
  FROM Filter_532_to_Filter_534 AS in0
  
  GROUP BY 
    idf_pers_ods_contrato, numero_contrato, descripcion_crm

),

Join_538_left_UnionLeftOuter AS (

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
    in0.*
  
  FROM Summarize_533 AS in0
  LEFT JOIN Summarize_537 AS in1
     ON (
      (in0.idf_pers_ods_contrato = in1.idf_pers_ods_contrato)
      AND (in0.numero_contrato = in1.numero_contrato)
    )

),

Formula_535_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((cantidad_tarjetas_con_consumo IS NULL) OR ((LENGTH(cantidad_tarjetas_con_consumo)) = 0))
          THEN '0'
        ELSE cantidad_tarjetas_con_consumo
      END
    ) AS INT64) AS cantidad_tarjetas_con_consumo,
    21 AS CodProducto,
    CAST(descripcion_crm AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Cantidad de Tarjetas: ', 
        (
          CASE
            WHEN ((cantidad_tarjetas IS NULL) OR ((LENGTH(CAST(cantidad_tarjetas AS STRING))) = 0))
              THEN 0
            ELSE (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', cantidad_tarjetas)), ',', '__THS__')), '__THS__', ''))
          END
        ))
    ) AS STRING) AS Col2,
    * EXCEPT (`cantidad_tarjetas_con_consumo`)
  
  FROM Join_538_left_UnionLeftOuter AS in0

),

Formula_535_1 AS (

  SELECT 
    CAST((
      CONCAT(
        'Cantidad de Tarjetas con Consumo: ', 
        (
          CASE
            WHEN (
              (cantidad_tarjetas_con_consumo IS NULL)
              OR ((LENGTH(CAST(cantidad_tarjetas_con_consumo AS STRING))) = 0)
            )
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT('%.0f', CAST(cantidad_tarjetas_con_consumo AS FLOAT64))), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS STRING) AS Col3,
    CAST((
      CONCAT(
        'Cantidad transacciones mes: ', 
        (
          CASE
            WHEN ((cantidad_compras_totales IS NULL) OR ((LENGTH(CAST(cantidad_compras_totales AS STRING))) = 0))
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT('%.0f', CAST(cantidad_compras_totales AS FLOAT64))), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS STRING) AS Col4,
    CAST((
      CONCAT(
        'Cantidad transacciones 3 meses: ', 
        (
          CASE
            WHEN (
              (cantidad_compras_totales_3meses IS NULL)
              OR ((LENGTH(CAST(cantidad_compras_totales_3meses AS STRING))) = 0)
            )
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT('%.0f', CAST(cantidad_compras_totales_3meses AS FLOAT64))), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS STRING) AS Col5,
    *
  
  FROM Formula_535_0 AS in0

),

AlteryxSelect_540 AS (

  SELECT 
    idf_pers_ods_contrato AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5
  
  FROM Formula_535_1 AS in0

),

Union_49_reformat_4 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5
  
  FROM AlteryxSelect_540 AS in0

),

aka_Server_UYDB_201 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_201_ref') }}

),

Filter_195 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_201 AS in0
  
  WHERE ((STRPOS(nombre_especie_valor, 'fondo')) > 0)

),

aka_Server_UYDB_194 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_194_ref') }}

),

Join_202_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`codigo_valor`)
  
  FROM aka_Server_UYDB_194 AS in0
  INNER JOIN Filter_195 AS in1
     ON (in0.codigo_valor = in1.codigo_valor)

),

Summarize_197 AS (

  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Join_202_inner AS in0

),

Formula_198_0 AS (

  SELECT 
    8 AS CodProducto,
    *
  
  FROM Summarize_197 AS in0

),

Union_49_reformat_22 AS (

  SELECT CAST(CodProducto AS STRING) AS CodProducto
  
  FROM Formula_198_0 AS in0

),

aka_Server_UYDB_830 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_830_ref') }}

),

Filter_833 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_830 AS in0
  
  WHERE (
          (fecha_ultima_transaccion >= (PARSE_DATE('%Y-%m-%d', (DATE_ADD(CURRENT_DATE, INTERVAL -3 MONTH)))))
          AND (NOT((idf_pers_ods IS NULL) OR ((LENGTH(idf_pers_ods)) = 0)))
        )

),

Summarize_835 AS (

  SELECT 
    MIN(fecha_activacion) AS fecha_activacion,
    MIN(fecha_primera_transaccion) AS fecha_primera_transaccion,
    MAX(fecha_ultima_transaccion) AS fecha_ultima_transaccion,
    SUM(monto_transacciones_mes_actual) AS monto_transacciones_mes_actual,
    banco_cuenta AS banco_cuenta,
    idf_pers_ods AS idf_pers_ods
  
  FROM Filter_833 AS in0
  
  GROUP BY 
    banco_cuenta, idf_pers_ods

),

Summarize_834 AS (

  SELECT 
    SUM(monto_transacciones_mes_actual) AS monto_transacciones_mes_actual,
    (ARRAY_TO_STRING((ARRAY_AGG(banco_cuenta)), ',')) AS banco_cuenta,
    FIRST_VALUE(fecha_activacion) AS fecha_activacion,
    FIRST_VALUE(fecha_primera_transaccion) AS fecha_primera_transaccion,
    FIRST_VALUE(fecha_ultima_transaccion) AS fecha_ultima_transaccion,
    idf_pers_ods AS idf_pers_ods
  
  FROM Summarize_835 AS in0
  
  GROUP BY idf_pers_ods

),

Formula_831_0 AS (

  SELECT 
    CAST(INITCAP(banco_cuenta) AS STRING) AS banco_cuenta,
    34 AS CodProducto,
    * EXCEPT (`banco_cuenta`)
  
  FROM Summarize_834 AS in0

),

Formula_831_1 AS (

  SELECT 
    CAST((CONCAT('Banco cobro: ', banco_cuenta)) AS STRING) AS Col1,
    CAST((CONCAT('Fecha activacion: ', (FORMAT_TIMESTAMP('%d-%m-%Y', fecha_activacion)))) AS STRING) AS Col2,
    CAST((CONCAT('Fecha primera transaccion: ', (FORMAT_TIMESTAMP('%d-%m-%Y', fecha_primera_transaccion)))) AS STRING) AS Col3,
    CAST((CONCAT('Fecha ultima transaccion: ', (FORMAT_TIMESTAMP('%d-%m-%Y', fecha_ultima_transaccion)))) AS STRING) AS Col4,
    CAST((
      CONCAT(
        'Monto transacciones mes (Arbitrado UYU): ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.2f', CAST(monto_transacciones_mes_actual AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col5,
    *
  
  FROM Formula_831_0 AS in0

),

AlteryxSelect_832 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5
  
  FROM Formula_831_1 AS in0

),

Union_49_reformat_19 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5
  
  FROM AlteryxSelect_832 AS in0

),

Filter_611 AS (

  SELECT * 
  
  FROM Join_829_inner AS in0
  
  WHERE (tipo_transaccion = 'Compra TD')

),

Summarize_613 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((monto_arbitrado_dolares IS NULL) OR (CAST(monto_arbitrado_dolares AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS cantidad_de_transacciones,
    SUM(monto_arbitrado_dolares) AS monto_arbitrado_dolares,
    AAAAMM AS AAAAMM,
    idf_pers_ods AS idf_pers_ods,
    pan AS pan
  
  FROM Filter_611 AS in0
  
  GROUP BY 
    AAAAMM, idf_pers_ods, pan

),

Formula_612_0 AS (

  SELECT 
    5 AS CodProducto,
    CAST((
      CONCAT(
        'PAN: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(pan AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Mes: ', 
        (
          SUBSTRING(
            CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(AAAAMM AS FLOAT64))), ',', '__THS__')), '__THS__', '')) AS STRING), 
            5, 
            2)
        ), 
        '-', 
        (
          SUBSTRING(
            CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(AAAAMM AS FLOAT64))), ',', '__THS__')), '__THS__', '')) AS STRING), 
            1, 
            4)
        ))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT(
        'Cantidad Transacciones: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', cantidad_de_transacciones)), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Col3,
    CAST((
      CONCAT(
        'Volumen de Transacciones: USD ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.2f', CAST(monto_arbitrado_dolares AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col4,
    *
  
  FROM Summarize_613 AS in0

),

AlteryxSelect_614 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4
  
  FROM Formula_612_0 AS in0

),

Union_49_reformat_11 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4
  
  FROM AlteryxSelect_614 AS in0

),

Formula_508_0 AS (

  SELECT 
    14 AS CodProducto,
    CAST(descripcion_crm AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Fecha Alta: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(EXTRACT(DAY FROM fecha_alta_contrato) AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            '')
        ), 
        '-', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(EXTRACT(MONTH FROM fecha_alta_contrato) AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            '')
        ), 
        '-', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(EXTRACT(YEAR FROM fecha_alta_contrato) AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            '')
        ))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT(
        'Limite: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.2f', CAST(limite_tarjeta AS FLOAT64))), ',', '__THS__')), '__THS__', ',')))
    ) AS STRING) AS Col3,
    CAST((
      CONCAT('Tipo tarjeta: ', (
        CASE
          WHEN es_titular
            THEN 'Titular  '
          ELSE 'Adicional'
        END
      ))
    ) AS STRING) AS Col4,
    CAST((CONCAT('Pan: ', pan)) AS STRING) AS Col5,
    CAST((CONCAT('Cuenta: ', numero_contrato)) AS STRING) AS Col6,
    CAST((
      CONCAT(
        'Estado: ', 
        (
          CASE
            WHEN (
              (((NOT bloqueo_soft_contrato) AND (NOT bloqueo_duro_contrato)) AND (NOT bloqueo_soft_tarjeta))
              AND (NOT bloqueo_duro_tarjeta)
            )
              THEN ' Ok       '
            ELSE ' Bloqueado'
          END
        ))
    ) AS STRING) AS Col7,
    CAST((
      CONCAT(
        'Cantidad transacciones mes: ', 
        (
          CASE
            WHEN ((cantidad_compras_totales IS NULL) OR ((LENGTH(CAST(cantidad_compras_totales AS STRING))) = 0))
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT('%.0f', CAST(cantidad_compras_totales AS FLOAT64))), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS STRING) AS Col8,
    CAST((
      CONCAT(
        'Cantidad transacciones 3 mes: ', 
        (
          CASE
            WHEN (
              (cantidad_compras_totales_3meses IS NULL)
              OR ((LENGTH(CAST(cantidad_compras_totales_3meses AS STRING))) = 0)
            )
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT('%.0f', CAST(cantidad_compras_totales_3meses AS FLOAT64))), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS STRING) AS Col9,
    *
  
  FROM Filter_507 AS in0

),

Filter_522 AS (

  SELECT * 
  
  FROM Formula_508_0 AS in0
  
  WHERE (
          (
            (
              (codigo_producto = '77')
              AND (
                    (
                      ((CAST(codigo_subproducto AS STRING) = '3095') OR (CAST(codigo_subproducto AS STRING) = '3195'))
                      OR (CAST(codigo_subproducto AS STRING) = '3097')
                    )
                    OR (CAST(codigo_subproducto AS STRING) = '3004')
                  )
            )
            OR (codigo_producto = '80')
          )
          AND (
                (
                  ((CAST(codigo_subproducto AS STRING) = '3096') OR (CAST(codigo_subproducto AS STRING) = '3196'))
                  OR (CAST(codigo_subproducto AS STRING) = '3098')
                )
                OR (CAST(codigo_subproducto AS STRING) = '3005')
              )
        )

),

Filter_519 AS (

  SELECT * 
  
  FROM Join_506_inner AS in0
  
  WHERE (
          (codigo_producto = '80')
          AND (
                (
                  ((CAST(codigo_subproducto AS STRING) = '3096') OR (CAST(codigo_subproducto AS STRING) = '3196'))
                  OR (CAST(codigo_subproducto AS STRING) = '3098')
                )
                OR (CAST(codigo_subproducto AS STRING) = '3005')
              )
        )

),

Summarize_516 AS (

  SELECT DISTINCT idf_pers_ods_tarjeta AS idf_pers_ods
  
  FROM Filter_519 AS in0

),

Filter_520 AS (

  SELECT * 
  
  FROM Join_506_inner AS in0
  
  WHERE (
          (codigo_producto = '77')
          AND (
                (
                  ((CAST(codigo_subproducto AS STRING) = '3095') OR (CAST(codigo_subproducto AS STRING) = '3195'))
                  OR (CAST(codigo_subproducto AS STRING) = '3097')
                )
                OR (CAST(codigo_subproducto AS STRING) = '3004')
              )
        )

),

Summarize_517 AS (

  SELECT DISTINCT idf_pers_ods_tarjeta AS idf_pers_ods
  
  FROM Filter_520 AS in0

),

JoinMultiple_514 AS (

  SELECT in0.idf_pers_ods AS idf_pers_ods
  
  FROM Summarize_516 AS in0
  INNER JOIN Summarize_517 AS in1
     ON (in0.idf_pers_ods = in1.idf_pers_ods)
  INNER JOIN Summarize_515 AS in2
     ON ((coalesce(IN0.IDF_PERS_ODS, IN1.IDF_PERS_ODS)) = IN2.IDF_PERS_ODS)

),

Join_521_inner AS (

  SELECT in0.*
  
  FROM Filter_522 AS in0
  INNER JOIN JoinMultiple_514 AS in1
     ON (in0.idf_pers_ods_tarjeta = in1.idf_pers_ods)

),

Filter_507_reject AS (

  SELECT * 
  
  FROM Join_506_inner AS in0
  
  WHERE ((tipo_tarjeta <> 'TC') OR ((tipo_tarjeta = 'TC') IS NULL))

),

Formula_511_0 AS (

  SELECT 
    15 AS CodProducto,
    CAST(descripcion_crm AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Fecha Alta: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(EXTRACT(DAY FROM fecha_alta_contrato) AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            '')
        ), 
        '-', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(EXTRACT(MONTH FROM fecha_alta_contrato) AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            '')
        ), 
        '-', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(EXTRACT(YEAR FROM fecha_alta_contrato) AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            '')
        ))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT('Tipo tarjeta: ', (
        CASE
          WHEN es_titular
            THEN 'Titular  '
          ELSE 'Adicional'
        END
      ))
    ) AS STRING) AS Col3,
    CAST((CONCAT('Pan: ', pan)) AS STRING) AS Col4,
    CAST((CONCAT('Cuenta: ', numero_contrato)) AS STRING) AS Col5,
    CAST((
      CONCAT(
        'Estado: ', 
        (
          CASE
            WHEN (
              (((NOT bloqueo_soft_contrato) AND (NOT bloqueo_duro_contrato)) AND (NOT bloqueo_soft_tarjeta))
              AND (NOT bloqueo_duro_tarjeta)
            )
              THEN ' Ok       '
            ELSE ' Bloqueado'
          END
        ))
    ) AS STRING) AS Col6,
    CAST((
      CONCAT(
        'Cantidad transacciones mes: ', 
        (
          CASE
            WHEN ((cantidad_compras_totales IS NULL) OR ((LENGTH(CAST(cantidad_compras_totales AS STRING))) = 0))
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT('%.0f', CAST(cantidad_compras_totales AS FLOAT64))), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS STRING) AS Col7,
    CAST((
      CONCAT(
        'Cantidad transacciones 3 mes: ', 
        (
          CASE
            WHEN (
              (cantidad_compras_totales_3meses IS NULL)
              OR ((LENGTH(CAST(cantidad_compras_totales_3meses AS STRING))) = 0)
            )
              THEN 0
            ELSE (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT('%.0f', CAST(cantidad_compras_totales_3meses AS FLOAT64))), ',', '__THS__')), 
                '__THS__', 
                '')
            )
          END
        ))
    ) AS STRING) AS Col8,
    *
  
  FROM Filter_507_reject AS in0

),

Filter_523 AS (

  SELECT * 
  
  FROM Formula_511_0 AS in0
  
  WHERE (
          (codigo_producto = '88')
          AND (
                ((CAST(codigo_subproducto AS STRING) = '5601') OR (CAST(codigo_subproducto AS STRING) = '7601'))
                OR (CAST(codigo_subproducto AS STRING) = '7603')
              )
        )

),

Join_524_inner AS (

  SELECT in0.*
  
  FROM Filter_523 AS in0
  INNER JOIN JoinMultiple_514 AS in1
     ON (in0.idf_pers_ods_tarjeta = in1.idf_pers_ods)

),

Union_525 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_521_inner', 'Join_524_inner'], 
      [
        '[{"name": "idf_pers_ods_tarjeta", "dataType": "String"}, {"name": "motivo_baja_contrato", "dataType": "String"}, {"name": "numero_contrato", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "es_titular", "dataType": "Boolean"}, {"name": "bloqueo_soft_tarjeta", "dataType": "Boolean"}, {"name": "Col9", "dataType": "String"}, {"name": "idf_pers_ods_contrato", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "bloqueo_duro_contrato", "dataType": "Boolean"}, {"name": "cantidad_compras_totales", "dataType": "Integer"}, {"name": "fecha_baja_contrato", "dataType": "Date"}, {"name": "codigo_subproducto", "dataType": "String"}, {"name": "codigo_bloqueo_contrato", "dataType": "Double"}, {"name": "cantidad_compras_totales_3meses", "dataType": "Integer"}, {"name": "tipo_tarjeta", "dataType": "String"}, {"name": "fecha_alta_contrato", "dataType": "Date"}, {"name": "limite_tarjeta", "dataType": "Decimal"}, {"name": "Col5", "dataType": "String"}, {"name": "bloqueo_soft_contrato", "dataType": "Boolean"}, {"name": "condicion_economica", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "tarjeta_transaccional_mes", "dataType": "Boolean"}, {"name": "codigo_destino", "dataType": "String"}, {"name": "descripcion_crm", "dataType": "String"}, {"name": "codigo_producto", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "pan", "dataType": "String"}, {"name": "sub_tipo_producto_comercial", "dataType": "String"}, {"name": "bloqueo_duro_tarjeta", "dataType": "Boolean"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "idf_pers_ods_tarjeta", "dataType": "String"}, {"name": "motivo_baja_contrato", "dataType": "String"}, {"name": "numero_contrato", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "es_titular", "dataType": "Boolean"}, {"name": "bloqueo_soft_tarjeta", "dataType": "Boolean"}, {"name": "idf_pers_ods_contrato", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "bloqueo_duro_contrato", "dataType": "Boolean"}, {"name": "cantidad_compras_totales", "dataType": "Integer"}, {"name": "fecha_baja_contrato", "dataType": "Date"}, {"name": "codigo_subproducto", "dataType": "String"}, {"name": "codigo_bloqueo_contrato", "dataType": "Double"}, {"name": "cantidad_compras_totales_3meses", "dataType": "Integer"}, {"name": "tipo_tarjeta", "dataType": "String"}, {"name": "fecha_alta_contrato", "dataType": "Date"}, {"name": "limite_tarjeta", "dataType": "Decimal"}, {"name": "Col5", "dataType": "String"}, {"name": "bloqueo_soft_contrato", "dataType": "Boolean"}, {"name": "condicion_economica", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "tarjeta_transaccional_mes", "dataType": "Boolean"}, {"name": "codigo_destino", "dataType": "String"}, {"name": "descripcion_crm", "dataType": "String"}, {"name": "codigo_producto", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "pan", "dataType": "String"}, {"name": "sub_tipo_producto_comercial", "dataType": "String"}, {"name": "bloqueo_duro_tarjeta", "dataType": "Boolean"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_526_0 AS (

  SELECT 
    1 AS CodProducto,
    * EXCEPT (`codproducto`)
  
  FROM Union_525 AS in0

),

AlteryxSelect_527 AS (

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
  
  FROM Formula_526_0 AS in0

),

Union_49_reformat_3 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5,
    CAST(Col6 AS STRING) AS Col6,
    CAST(Col7 AS STRING) AS Col7,
    CAST(Col8 AS STRING) AS Col8,
    CAST(Col9 AS STRING) AS Col9
  
  FROM AlteryxSelect_527 AS in0

),

Filter_647 AS (

  SELECT * 
  
  FROM Join_639_inner AS in0
  
  WHERE (
          ((tipo_producto_comercial <> 'Coche') OR ((tipo_producto_comercial = 'Coche') IS NULL))
          AND CAST(((STRPOS(tipo_producto_comercial, 'GTM')) > 0) AS BOOL)
        )

),

Formula_294_0 AS (

  SELECT 
    25 AS CodProducto,
    CAST((
      CONCAT(
        'Destino: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(codigo_destino AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ' ', 
        descripcion_crm)
    ) AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Monto: ', 
        divisa, 
        ' ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(saldo_moneda_origen AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT(
        'Op.: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(numero_contrato AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Col3,
    *
  
  FROM Filter_647 AS in0

),

AlteryxSelect_295 AS (

  SELECT 
    CAST(idf_pers_ods AS STRING) AS IDF_PERS_ODS,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3
  
  FROM Formula_294_0 AS in0

),

Union_49_reformat_7 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM AlteryxSelect_295 AS in0

),

aka_Server_UYDB_597 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_597_ref') }}

),

Filter_602 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_597 AS in0
  
  WHERE ((tipo_servicio = 'DA - Tarjeta en Cuenta') AND (esta_activo = TRUE))

),

Formula_603_0 AS (

  SELECT 
    '2' AS CodProducto,
    CAST(tipo_servicio AS STRING) AS Col1,
    CAST((CONCAT('Fecha Alta: ', fecha_alta)) AS STRING) AS Col2,
    CAST((
      CONCAT('Estado: ', (
        CASE
          WHEN esta_activo
            THEN 'Activo   '
          ELSE 'Cancelado'
        END
      ))
    ) AS STRING) AS Col3,
    CAST((CONCAT('Contrato: ', numero_referencia)) AS STRING) AS Col4,
    CAST((
      CONCAT(
        'Cuenta Cargo UYU: ', 
        (
          SUBSTRING(
            CAST((SUBSTRING(cargo_cuenta_pesos, 1, 8)) AS STRING), 
            (((LENGTH(CAST((SUBSTRING(cargo_cuenta_pesos, 1, 8)) AS STRING))) - 4) + 1), 
            4)
        ), 
        '-', 
        (SUBSTRING(cargo_cuenta_pesos, (((LENGTH(cargo_cuenta_pesos)) - 8) + 1), 8)))
    ) AS STRING) AS Col5,
    CAST((
      CONCAT(
        'Cuenta Cargo USD: ', 
        (
          SUBSTRING(
            CAST((SUBSTRING(cargo_cuenta_dolares, 1, 8)) AS STRING), 
            (((LENGTH(CAST((SUBSTRING(cargo_cuenta_dolares, 1, 8)) AS STRING))) - 4) + 1), 
            4)
        ), 
        '-', 
        (SUBSTRING(cargo_cuenta_dolares, (((LENGTH(cargo_cuenta_dolares)) - 8) + 1), 8)))
    ) AS STRING) AS Col6,
    *
  
  FROM Filter_602 AS in0

),

AlteryxSelect_604 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6
  
  FROM Formula_603_0 AS in0

),

Union_49_reformat_14 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5,
    CAST(Col6 AS STRING) AS Col6
  
  FROM AlteryxSelect_604 AS in0

),

Filter_654 AS (

  SELECT * 
  
  FROM Join_639_inner AS in0
  
  WHERE (
          (
            ((tipo_producto_comercial <> 'Coche') OR ((tipo_producto_comercial = 'Coche') IS NULL))
            AND (
                  (NOT CAST(((STRPOS(tipo_producto_comercial, 'GTM')) > 0) AS BOOL))
                  OR (CAST(((STRPOS(tipo_producto_comercial, 'GTM')) > 0) AS BOOL) IS NULL)
                )
          )
          AND CAST(((STRPOS(tipo_producto_comercial, 'Leasing')) > 0) AS BOOL)
        )

),

Formula_650_0 AS (

  SELECT 
    28 AS CodProducto,
    CAST((
      CONCAT(
        'Destino: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(codigo_destino AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ' ', 
        descripcion_crm)
    ) AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Monto: ', 
        divisa, 
        ' ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(saldo_moneda_origen AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT(
        'Op.: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(numero_contrato AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Col3,
    *
  
  FROM Filter_654 AS in0

),

AlteryxSelect_649 AS (

  SELECT 
    CAST(idf_pers_ods AS STRING) AS IDF_PERS_ODS,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3
  
  FROM Formula_650_0 AS in0

),

Union_49_reformat_8 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM AlteryxSelect_649 AS in0

),

Summarize_676 AS (

  SELECT 
    SUM(monto_arbitrado_dolares) AS monto_arbitrado_dolares,
    merchant AS merchant,
    idf_pers_ods AS idf_pers_ods,
    AAAAMM AS AAAAMM,
    MesId AS MesID
  
  FROM Join_680_inner AS in0
  
  GROUP BY 
    merchant, idf_pers_ods, AAAAMM, MesId

),

Sort_687 AS (

  SELECT * 
  
  FROM AppendFields_686 AS in0
  
  ORDER BY idf_pers_ods ASC, AAAAMM ASC

),

Join_668_inner_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (
          ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.AAAAMM = in1.AAAAMM))
          AND (in0.merchant = in1.merchant)
        )
          THEN NULL
        ELSE 0
      END
    ) AS monto_arbitrado_dolares,
    (
      CASE
        WHEN (
          ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.AAAAMM = in1.AAAAMM))
          AND (in0.merchant = in1.merchant)
        )
          THEN NULL
        ELSE in1.AAAAMM
      END
    ) AS AAAAMM,
    (
      CASE
        WHEN (
          ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.AAAAMM = in1.AAAAMM))
          AND (in0.merchant = in1.merchant)
        )
          THEN NULL
        ELSE in1.idf_pers_ods
      END
    ) AS idf_pers_ods,
    (
      CASE
        WHEN (
          ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.AAAAMM = in1.AAAAMM))
          AND (in0.merchant = in1.merchant)
        )
          THEN NULL
        ELSE in1.merchant
      END
    ) AS merchant,
    in1.* EXCEPT (`AAAAMM`, `idf_pers_ods`, `merchant`)
  
  FROM Summarize_676 AS in0
  RIGHT JOIN Sort_687 AS in1
     ON (
      ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.AAAAMM = in1.AAAAMM))
      AND (in0.merchant = in1.merchant)
    )

),

MultiFieldFormula_678 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['Join_668_inner_UnionRightOuter'], 
      "CASE WHEN (column_value = NULL) THEN CASE WHEN (column_value = 0) THEN -1 ELSE 0 END ELSE column_value END", 
      ['MesID', 'monto_arbitrado_dolares', 'idf_pers_ods', 'merchant', 'AAAAMM'], 
      ['monto_arbitrado_dolares'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Formula_670_to_Formula_674_0 AS (

  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(AAAAMM AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(monto_arbitrado_dolares AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS MontoDolaresAAAAMM,
    (MesID + 1) AS MesID,
    * EXCEPT (`mesid`)
  
  FROM MultiFieldFormula_678 AS in0

),

Filter_675 AS (

  SELECT * 
  
  FROM Formula_670_to_Formula_674_0 AS in0
  
  WHERE (MesID < CAST('19' AS INT64))

),

CrossTab_671 AS (

  SELECT *
  
  FROM (
    SELECT 
      merchant,
      idf_pers_ods,
      MesID,
      MONTODOLARESAAAAMM
    
    FROM Filter_675 AS in0
  )
  PIVOT (
    FIRST(MONTODOLARESAAAAMM) AS First
    FOR MesID
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'
    )
  )

),

DynamicRename_672 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_671'], 
      ['12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'], 
      'editPrefixSuffix', 
      [
        '12', 
        '8', 
        '4', 
        '15', 
        '11', 
        '9', 
        '13', 
        '16', 
        '5', 
        '10', 
        '6', 
        '17', 
        '14', 
        '2', 
        'idf_pers_ods', 
        '18', 
        '7', 
        'merchant', 
        '3'
      ], 
      'Prefix', 
      'Col', 
      ""
    )
  }}

),

Formula_673_0 AS (

  SELECT 
    19 AS CodProducto,
    CAST((CONCAT('Importe Cobrado ', merchant)) AS STRING) AS Col1,
    *
  
  FROM DynamicRename_672 AS in0

),

Filter_821 AS (

  SELECT * 
  
  FROM Formula_673_0 AS in0
  
  WHERE (
          (
            (
              (
                (
                  (CAST((SUBSTRING(Col2, (((LENGTH(Col2)) - 2) + 1), 2)) AS STRING) <> ';0')
                  OR ((SUBSTRING(Col2, (((LENGTH(Col2)) - 2) + 1), 2)) IS NULL)
                )
                OR (
                     (CAST((SUBSTRING(Col3, (((LENGTH(Col3)) - 2) + 1), 2)) AS STRING) <> ';0')
                     OR ((SUBSTRING(Col3, (((LENGTH(Col3)) - 2) + 1), 2)) IS NULL)
                   )
              )
              OR (
                   (
                     (CAST((SUBSTRING(Col4, (((LENGTH(Col4)) - 2) + 1), 2)) AS STRING) <> ';0')
                     OR ((SUBSTRING(Col4, (((LENGTH(Col4)) - 2) + 1), 2)) IS NULL)
                   )
                   OR (
                        (CAST((SUBSTRING(Col5, (((LENGTH(Col5)) - 2) + 1), 2)) AS STRING) <> ';0')
                        OR ((SUBSTRING(Col5, (((LENGTH(Col5)) - 2) + 1), 2)) IS NULL)
                      )
                 )
            )
            OR (
                 (
                   (
                     (CAST((SUBSTRING(Col6, (((LENGTH(Col6)) - 2) + 1), 2)) AS STRING) <> ';0')
                     OR ((SUBSTRING(Col6, (((LENGTH(Col6)) - 2) + 1), 2)) IS NULL)
                   )
                   OR (
                        (CAST((SUBSTRING(Col7, (((LENGTH(Col7)) - 2) + 1), 2)) AS STRING) <> ';0')
                        OR ((SUBSTRING(Col7, (((LENGTH(Col7)) - 2) + 1), 2)) IS NULL)
                      )
                 )
                 OR (
                      (
                        (CAST((SUBSTRING(Col8, (((LENGTH(Col8)) - 2) + 1), 2)) AS STRING) <> ';0')
                        OR ((SUBSTRING(Col8, (((LENGTH(Col8)) - 2) + 1), 2)) IS NULL)
                      )
                      OR (
                           (CAST((SUBSTRING(Col9, (((LENGTH(Col9)) - 2) + 1), 2)) AS STRING) <> ';0')
                           OR (
                                ((SUBSTRING(Col9, (((LENGTH(Col9)) - 2) + 1), 2)) IS NULL)
                                OR (CAST((SUBSTRING(Col10, (((LENGTH(Col10)) - 2) + 1), 2)) AS STRING) <> ';0')
                              )
                         )
                    )
               )
          )
          OR (
               (
                 (
                   (
                     ((SUBSTRING(Col10, (((LENGTH(Col10)) - 2) + 1), 2)) IS NULL)
                     OR (CAST((SUBSTRING(Col11, (((LENGTH(Col11)) - 2) + 1), 2)) AS STRING) <> ';0')
                   )
                   OR (
                        ((SUBSTRING(Col11, (((LENGTH(Col11)) - 2) + 1), 2)) IS NULL)
                        OR (CAST((SUBSTRING(Col12, (((LENGTH(Col12)) - 2) + 1), 2)) AS STRING) <> ';0')
                      )
                 )
                 OR (
                      (
                        ((SUBSTRING(Col12, (((LENGTH(Col12)) - 2) + 1), 2)) IS NULL)
                        OR (CAST((SUBSTRING(Col13, (((LENGTH(Col13)) - 2) + 1), 2)) AS STRING) <> ';0')
                      )
                      OR (
                           ((SUBSTRING(Col13, (((LENGTH(Col13)) - 2) + 1), 2)) IS NULL)
                           OR (CAST((SUBSTRING(Col14, (((LENGTH(Col14)) - 2) + 1), 2)) AS STRING) <> ';0')
                         )
                    )
               )
               OR (
                    (
                      (
                        ((SUBSTRING(Col14, (((LENGTH(Col14)) - 2) + 1), 2)) IS NULL)
                        OR (CAST((SUBSTRING(Col15, (((LENGTH(Col15)) - 2) + 1), 2)) AS STRING) <> ';0')
                      )
                      OR (
                           ((SUBSTRING(Col15, (((LENGTH(Col15)) - 2) + 1), 2)) IS NULL)
                           OR (CAST((SUBSTRING(Col16, (((LENGTH(Col16)) - 2) + 1), 2)) AS STRING) <> ';0')
                         )
                    )
                    OR (
                         (
                           ((SUBSTRING(Col16, (((LENGTH(Col16)) - 2) + 1), 2)) IS NULL)
                           OR (CAST((SUBSTRING(Col17, (((LENGTH(Col17)) - 2) + 1), 2)) AS STRING) <> ';0')
                         )
                         OR (
                              ((SUBSTRING(Col17, (((LENGTH(Col17)) - 2) + 1), 2)) IS NULL)
                              OR (
                                   (CAST((SUBSTRING(Col18, (((LENGTH(Col18)) - 2) + 1), 2)) AS STRING) <> ';0')
                                   OR ((SUBSTRING(Col18, (((LENGTH(Col18)) - 2) + 1), 2)) IS NULL)
                                 )
                            )
                       )
                  )
             )
        )

),

AlteryxSelect_677 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6,
    Col7 AS Col7,
    Col8 AS Col8,
    Col9 AS Col9,
    Col10 AS Col10,
    Col11 AS Col11,
    Col12 AS Col12,
    Col13 AS Col13,
    Col14 AS Col14,
    Col15 AS Col15,
    Col16 AS Col16,
    Col17 AS Col17,
    Col18 AS Col18
  
  FROM Filter_821 AS in0

),

Union_49_reformat_9 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col10 AS STRING) AS Col10,
    CAST(Col11 AS STRING) AS Col11,
    CAST(Col12 AS STRING) AS Col12,
    CAST(Col13 AS STRING) AS Col13,
    CAST(Col14 AS STRING) AS Col14,
    CAST(Col15 AS STRING) AS Col15,
    CAST(Col16 AS STRING) AS Col16,
    CAST(Col17 AS STRING) AS Col17,
    CAST(Col18 AS STRING) AS Col18,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5,
    CAST(Col6 AS STRING) AS Col6,
    CAST(Col7 AS STRING) AS Col7,
    CAST(Col8 AS STRING) AS Col8,
    CAST(Col9 AS STRING) AS Col9
  
  FROM AlteryxSelect_677 AS in0

),

Filter_737_to_Filter_407 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_311 AS in0
  
  WHERE (
          (
            ((codigo_agrupacion_contable = 'CONTINGENCIAS') AND (codigo_producto = '55'))
            AND (sector_cuenta_contable = '04')
          )
          AND (capitulo_cuenta_contable = '1')
        )

),

Summarize_406 AS (

  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Filter_737_to_Filter_407 AS in0

),

Formula_404_0 AS (

  SELECT 
    29 AS CodProducto,
    *
  
  FROM Summarize_406 AS in0

),

Union_49_reformat_27 AS (

  SELECT CAST(CodProducto AS STRING) AS CodProducto
  
  FROM Formula_404_0 AS in0

),

Filter_308 AS (

  SELECT * 
  
  FROM Join_639_inner AS in0
  
  WHERE (
          (
            (
              ((tipo_producto_comercial <> 'Coche') OR ((tipo_producto_comercial = 'Coche') IS NULL))
              AND (
                    (NOT CAST(((STRPOS(tipo_producto_comercial, 'GTM')) > 0) AS BOOL))
                    OR (CAST(((STRPOS(tipo_producto_comercial, 'GTM')) > 0) AS BOOL) IS NULL)
                  )
            )
            AND (
                  (NOT CAST(((STRPOS(tipo_producto_comercial, 'Leasing')) > 0) AS BOOL))
                  OR (CAST(((STRPOS(tipo_producto_comercial, 'Leasing')) > 0) AS BOOL) IS NULL)
                )
          )
          AND (grupo_producto_comercial = 'HIpotecarios')
        )

),

Summarize_307 AS (

  SELECT DISTINCT idf_pers_ods AS IDF_PERS_ODS
  
  FROM Filter_308 AS in0

),

Formula_305_0 AS (

  SELECT 
    13 AS CodProducto,
    *
  
  FROM Summarize_307 AS in0

),

Union_49_reformat_25 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM Formula_305_0 AS in0

),

Formula_698_0 AS (

  SELECT 
    27 AS CodProducto,
    CAST((
      CONCAT(
        'Cantidad operaciones: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', cantidad_operaciones)), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Monto a cobrar USD: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(saldo_arbitrado_dolares AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col2,
    *
  
  FROM Summarize_697 AS in0

),

AlteryxSelect_699 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2
  
  FROM Formula_698_0 AS in0

),

Union_49_reformat_16 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2
  
  FROM AlteryxSelect_699 AS in0

),

Filter_659 AS (

  SELECT * 
  
  FROM Join_639_inner AS in0
  
  WHERE (
          (
            (
              (
                ((tipo_producto_comercial <> 'Coche') OR ((tipo_producto_comercial = 'Coche') IS NULL))
                AND (
                      (NOT CAST(((STRPOS(tipo_producto_comercial, 'GTM')) > 0) AS BOOL))
                      OR (CAST(((STRPOS(tipo_producto_comercial, 'GTM')) > 0) AS BOOL) IS NULL)
                    )
              )
              AND (
                    (NOT CAST(((STRPOS(tipo_producto_comercial, 'Leasing')) > 0) AS BOOL))
                    OR (CAST(((STRPOS(tipo_producto_comercial, 'Leasing')) > 0) AS BOOL) IS NULL)
                  )
            )
            AND (
                  (grupo_producto_comercial <> 'HIpotecarios')
                  OR ((grupo_producto_comercial = 'HIpotecarios') IS NULL)
                )
          )
          AND (grupo_producto_comercial = 'Consumo')
        )

),

Summarize_292 AS (

  SELECT DISTINCT idf_pers_ods AS IDF_PERS_ODS
  
  FROM Filter_659 AS in0

),

Formula_290_0 AS (

  SELECT 
    17 AS CodProducto,
    *
  
  FROM Summarize_292 AS in0

),

Union_49_reformat_24 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM Formula_290_0 AS in0

),

aka_Server_UYDB_740 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_740_ref') }}

),

Formula_741_0 AS (

  SELECT 
    31 AS CodProducto,
    CAST((CONCAT('Cuenta sobregiro: ', codigo_sucursal_operacion, '-', numero_contrato, '-', divisa)) AS STRING) AS Col1,
    CAST((
      CONCAT(
        'Acuerdo Sobregiro: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(acuerdo_sobregiro_moneda_origen AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col2,
    CAST((
      CONCAT(
        'Sobregiro autorizado tomado: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(sobregiro_autorizado_moneda_origen AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col3,
    CAST((
      CONCAT(
        'Sobregiro autorizado tomado: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(sobregiro_no_autorizado_moneda_origen AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col4,
    CAST((
      CONCAT(
        'Sobregiro disponible: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.0f', CAST(sobregiro_disponible_moneda_origen AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col5,
    *
  
  FROM aka_Server_UYDB_740 AS in0

),

AlteryxSelect_742 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5
  
  FROM Formula_741_0 AS in0

),

Union_49_reformat_18 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5
  
  FROM AlteryxSelect_742 AS in0

),

Formula_411_0 AS (

  select  26 as `CodProducto` , *  REPLACE( idf_pers_ods as `idf_pers_ods` ) from Summarize_664

),

Union_49_reformat_28 AS (

  SELECT CAST(CodProducto AS STRING) AS CodProducto
  
  FROM Formula_411_0 AS in0

),

Filter_842 AS (

  SELECT * 
  
  FROM Formula_841_0 AS in0
  
  WHERE (CodProducto <> CAST('99' AS INT64))

),

AlteryxSelect_843 AS (

  SELECT 
    idf_pers_ods_tarjeta AS idf_pers_ods,
    CodProducto AS CodProducto
  
  FROM Filter_842 AS in0

),

Union_49_reformat_1 AS (

  SELECT CAST(CodProducto AS STRING) AS CodProducto
  
  FROM AlteryxSelect_843 AS in0

),

Summarize_191 AS (

  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM aka_Server_UYDB_194 AS in0

),

Formula_192_0 AS (

  SELECT 
    7 AS CodProducto,
    *
  
  FROM Summarize_191 AS in0

),

Union_49_reformat_21 AS (

  SELECT CAST(CodProducto AS STRING) AS CodProducto
  
  FROM Formula_192_0 AS in0

),

Filter_288 AS (

  SELECT * 
  
  FROM Join_639_inner AS in0
  
  WHERE (tipo_producto_comercial = 'Coche')

),

Summarize_286 AS (

  SELECT DISTINCT idf_pers_ods AS IDF_PERS_ODS
  
  FROM Filter_288 AS in0

),

Formula_284_0 AS (

  SELECT 
    12 AS CodProducto,
    *
  
  FROM Summarize_286 AS in0

),

Union_49_reformat_23 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM Formula_284_0 AS in0

),

AlteryxSelect_512 AS (

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
    Col8 AS Col8
  
  FROM Formula_511_0 AS in0

),

Union_49_reformat_2 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5,
    CAST(Col6 AS STRING) AS Col6,
    CAST(Col7 AS STRING) AS Col7,
    CAST(Col8 AS STRING) AS Col8
  
  FROM AlteryxSelect_512 AS in0

),

AlteryxSelect_509 AS (

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
  
  FROM Formula_508_0 AS in0

),

Union_49_reformat_0 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5,
    CAST(Col6 AS STRING) AS Col6,
    CAST(Col7 AS STRING) AS Col7,
    CAST(Col8 AS STRING) AS Col8,
    CAST(Col9 AS STRING) AS Col9
  
  FROM AlteryxSelect_509 AS in0

),

Formula_545_0 AS (

  SELECT 
    CAST(sub_tipo_producto AS STRING) AS Col1,
    CAST((CONCAT('Plan: ', TRIM(codigo_plan))) AS STRING) AS Col2,
    CAST((CONCAT('Desde Fecha: ', (FORMAT_TIMESTAMP('%d-%m-%Y', fecha_alta)))) AS STRING) AS Col3,
    CAST((
      CONCAT(
        'Cant. Emp.: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', cantidad_empleados)), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Col4,
    20 AS CodProducto,
    *
  
  FROM Filter_544 AS in0

),

AlteryxSelect_547 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4
  
  FROM Formula_545_0 AS in0

),

Union_49_reformat_12 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4
  
  FROM AlteryxSelect_547 AS in0

),

Summarize_573 AS (

  SELECT 
    SUM(importe_arbitrado_dolares) AS importe_arbitrado_dolares,
    AAAAMM AS AAAAMM,
    idf_pers_ods_cobrador AS idf_pers_ods
  
  FROM aka_Server_UYDB_551 AS in0
  
  GROUP BY 
    AAAAMM, idf_pers_ods_cobrador

),

Join_564_inner AS (

  SELECT 
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods AS idf_pers_ods,
    in0.importe_arbitrado_dolares AS importe_arbitrado_dolares,
    in1.MesId AS MesId
  
  FROM Summarize_573 AS in0
  INNER JOIN RecordID_563 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_565 AS (

  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Join_564_inner AS in0

),

AppendFields_566 AS (

  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_563 AS in0
  INNER JOIN Summarize_565 AS in1
     ON true

),

Sort_567 AS (

  SELECT * 
  
  FROM AppendFields_566 AS in0
  
  ORDER BY idf_pers_ods ASC, AAAAMM ASC

),

Summarize_559 AS (

  SELECT 
    SUM(importe_arbitrado_dolares) AS importe_arbitrado_dolares,
    idf_pers_ods AS idf_pers_ods,
    AAAAMM AS AAAAMM,
    MesId AS MesId
  
  FROM Join_564_inner AS in0
  
  GROUP BY 
    idf_pers_ods, AAAAMM, MesId

),

Join_569_inner_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE in1.idf_pers_ods
      END
    ) AS idf_pers_ods,
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE in1.MesId
      END
    ) AS MesId,
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE in1.AAAAMM
      END
    ) AS AAAAMM,
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE 0
      END
    ) AS importe_arbitrado_dolares
  
  FROM Summarize_559 AS in0
  RIGHT JOIN Sort_567 AS in1
     ON (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))

),

Formula_571_to_Formula_557_0 AS (

  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(AAAAMM AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.2f', CAST(importe_arbitrado_dolares AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS MontoDolaresAAAAMM,
    (CONCAT(MesId, 1)) AS MesId,
    * EXCEPT (`mesid`)
  
  FROM Join_569_inner_UnionRightOuter AS in0

),

Filter_558 AS (

  SELECT * 
  
  FROM Formula_571_to_Formula_557_0 AS in0
  
  WHERE (MesId < CAST('19' AS INT64))

),

CrossTab_553 AS (

  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesId,
      MONTODOLARESAAAAMM
    
    FROM Filter_558 AS in0
  )
  PIVOT (
    CONCAT_WS(MONTODOLARESAAAAMM) AS Concat
    FOR MesId
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'
    )
  )

),

DynamicRename_554 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_553'], 
      ['12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'], 
      'editPrefixSuffix', 
      [
        '12', 
        '8', 
        '4', 
        '15', 
        '11', 
        '9', 
        '13', 
        '16', 
        '5', 
        '10', 
        '6', 
        '17', 
        '14', 
        '2', 
        'idf_pers_ods', 
        '18', 
        '7', 
        '3'
      ], 
      'Prefix', 
      'Col', 
      ""
    )
  }}

),

Formula_555_0 AS (

  SELECT 
    9 AS CodProducto,
    'Importe Cobrado' AS Col1,
    *
  
  FROM DynamicRename_554 AS in0

),

Union_49_reformat_29 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col10 AS STRING) AS Col10,
    CAST(Col11 AS STRING) AS Col11,
    CAST(Col12 AS STRING) AS Col12,
    CAST(Col13 AS STRING) AS Col13,
    CAST(Col14 AS STRING) AS Col14,
    CAST(Col15 AS STRING) AS Col15,
    CAST(Col16 AS STRING) AS Col16,
    CAST(Col17 AS STRING) AS Col17,
    CAST(Col18 AS STRING) AS Col18,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5,
    CAST(Col6 AS STRING) AS Col6,
    CAST(Col7 AS STRING) AS Col7,
    CAST(Col8 AS STRING) AS Col8,
    CAST(Col9 AS STRING) AS Col9
  
  FROM Formula_555_0 AS in0

),

Filter_599 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_597 AS in0
  
  WHERE (
          (
            (CAST(tipo_servicio AS STRING) = 'DA - Servicio en Tarjeta')
            OR (CAST(tipo_servicio AS STRING) = 'DA - Servicio en Cuenta')
          )
          AND (esta_activo = TRUE)
        )

),

Formula_598_0 AS (

  SELECT 
    3 AS CodProducto,
    CAST(tipo_servicio AS STRING) AS Col1,
    CAST((CONCAT('Servicio: ', TRIM(nombre_proveedor))) AS STRING) AS Col2,
    CAST((
      CONCAT(
        'Monto abonado mes USD: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.2f', CAST(importe_abonado_arbitrado_dolares AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS Col3,
    CAST((
      CONCAT(
        'Cuenta Cargo UYU: ', 
        (
          CASE
            WHEN (tipo_servicio = 'DA - Servicio en Tarjeta')
              THEN cargo_cuenta_pesos
            ELSE CAST((
              CONCAT(
                (
                  SUBSTRING(
                    CAST((SUBSTRING(cargo_cuenta_pesos, 1, 8)) AS STRING), 
                    (((LENGTH(CAST((SUBSTRING(cargo_cuenta_pesos, 1, 8)) AS STRING))) - 4) + 1), 
                    4)
                ), 
                '-', 
                (SUBSTRING(cargo_cuenta_pesos, (((LENGTH(cargo_cuenta_pesos)) - 8) + 1), 8)))
            ) AS STRING)
          END
        ))
    ) AS STRING) AS Col4,
    CAST((
      CONCAT(
        'Cuenta Cargo USD: ', 
        (
          CASE
            WHEN (tipo_servicio = 'DA - Servicio en Tarjeta')
              THEN cargo_cuenta_dolares
            ELSE CAST((
              CONCAT(
                (
                  SUBSTRING(
                    CAST((SUBSTRING(cargo_cuenta_dolares, 1, 8)) AS STRING), 
                    (((LENGTH(CAST((SUBSTRING(cargo_cuenta_dolares, 1, 8)) AS STRING))) - 4) + 1), 
                    4)
                ), 
                '-', 
                (SUBSTRING(cargo_cuenta_dolares, (((LENGTH(cargo_cuenta_dolares)) - 8) + 1), 8)))
            ) AS STRING)
          END
        ))
    ) AS STRING) AS Col5,
    *
  
  FROM Filter_599 AS in0

),

AlteryxSelect_600 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5
  
  FROM Formula_598_0 AS in0

),

Union_49_reformat_5 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5
  
  FROM AlteryxSelect_600 AS in0

),

Sort_714 AS (

  SELECT * 
  
  FROM Summarize_713 AS in0
  
  ORDER BY AAAAMM DESC

),

Sample_715 AS (

  {{ prophecy_basics.Sample('Sort_714', [], 1002, 'firstN', 13) }}

),

RecordID_716 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `MesId`
  
  FROM Sample_715

),

Join_717_inner AS (

  SELECT 
    in0.monto_arbitrado_dolares AS monto_arbitrado_dolares,
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods AS idf_pers_ods,
    in0.TipoPago AS TipoPago,
    in1.MesId AS MesId
  
  FROM Formula_725_0 AS in0
  INNER JOIN RecordID_716 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_718 AS (

  SELECT 
    DISTINCT idf_pers_ods AS idf_pers_ods,
    TipoPago AS TipoPago
  
  FROM Join_717_inner AS in0

),

AppendFields_719 AS (

  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in1.TipoPago AS TipoPago,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_716 AS in0
  INNER JOIN Summarize_718 AS in1
     ON true

),

Sort_720 AS (

  SELECT * 
  
  FROM AppendFields_719 AS in0
  
  ORDER BY idf_pers_ods ASC, AAAAMM ASC

),

Summarize_712 AS (

  SELECT 
    SUM(monto_arbitrado_dolares) AS monto_arbitrado_dolares,
    idf_pers_ods AS idf_pers_ods,
    TipoPago AS TipoPago,
    AAAAMM AS AAAAMM,
    MesId AS MesId
  
  FROM Join_717_inner AS in0
  
  GROUP BY 
    idf_pers_ods, TipoPago, AAAAMM, MesId

),

Join_722_inner_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (
          (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          AND (in0.TipoPago = in1.TipoPago)
        )
          THEN NULL
        ELSE 0
      END
    ) AS monto_arbitrado_dolares,
    (
      CASE
        WHEN (
          (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          AND (in0.TipoPago = in1.TipoPago)
        )
          THEN NULL
        ELSE in1.AAAAMM
      END
    ) AS AAAAMM,
    (
      CASE
        WHEN (
          (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          AND (in0.TipoPago = in1.TipoPago)
        )
          THEN NULL
        ELSE in1.idf_pers_ods
      END
    ) AS idf_pers_ods,
    (
      CASE
        WHEN (
          (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          AND (in0.TipoPago = in1.TipoPago)
        )
          THEN NULL
        ELSE in1.TipoPago
      END
    ) AS TipoPago,
    (
      CASE
        WHEN (
          (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          AND (in0.TipoPago = in1.TipoPago)
        )
          THEN NULL
        ELSE in1.MesId
      END
    ) AS MesId
  
  FROM Summarize_712 AS in0
  RIGHT JOIN Sort_720 AS in1
     ON (
      (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
      AND (in0.TipoPago = in1.TipoPago)
    )

),

Formula_724_to_Formula_710_0 AS (

  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(AAAAMM AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT('%.2f', CAST(monto_arbitrado_dolares AS FLOAT64))), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS STRING) AS MontoDolaresAAAAMM,
    (CONCAT(MesId, 1)) AS MesId,
    * EXCEPT (`mesid`)
  
  FROM Join_722_inner_UnionRightOuter AS in0

),

Filter_711 AS (

  SELECT * 
  
  FROM Formula_724_to_Formula_710_0 AS in0
  
  WHERE (MesId < CAST('15' AS INT64))

),

CrossTab_706 AS (

  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      TipoPago,
      MesId,
      MONTODOLARESAAAAMM
    
    FROM Filter_711 AS in0
  )
  PIVOT (
    FIRST(MONTODOLARESAAAAMM) AS First
    FOR MesId
    IN (
      '12', '8', '4', '11', '9', '13', '5', '10', '6', '14', '2', '7', '3'
    )
  )

),

DynamicRename_707 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_706'], 
      ['12', '8', '4', '11', '9', '13', '5', '10', '6', '14', '2', '7', '3'], 
      'editPrefixSuffix', 
      ['12', '8', '4', '11', '9', 'TipoPago', '13', '5', '10', '6', '14', '2', 'idf_pers_ods', '7', '3'], 
      'Prefix', 
      'Col', 
      ""
    )
  }}

),

Formula_708_0 AS (

  SELECT 
    33 AS CodProducto,
    CAST(TipoPago AS STRING) AS Col1,
    *
  
  FROM DynamicRename_707 AS in0

),

AlteryxSelect_705 AS (

  select *  REPLACE( idf_pers_ods as `idf_pers_ods` ,  CodProducto as `CodProducto` ,  Col1 as `Col1` ,  Col2 as `Col2` ,  Col3 as `Col3` ,  Col4 as `Col4` ,  Col5 as `Col5` ,  Col6 as `Col6` ,  Col7 as `Col7` ,  Col8 as `Col8` ,  Col9 as `Col9` ,  Col10 as `Col10` ,  Col11 as `Col11` ,  Col12 as `Col12` ,  Col13 as `Col13` ,  Col14 as `Col14` ) from Formula_708_0

),

Union_49_reformat_17 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col10 AS STRING) AS Col10,
    CAST(Col11 AS STRING) AS Col11,
    CAST(Col12 AS STRING) AS Col12,
    CAST(Col13 AS STRING) AS Col13,
    CAST(Col14 AS STRING) AS Col14,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5,
    CAST(Col6 AS STRING) AS Col6,
    CAST(Col7 AS STRING) AS Col7,
    CAST(Col8 AS STRING) AS Col8,
    CAST(Col9 AS STRING) AS Col9
  
  FROM AlteryxSelect_705 AS in0

),

Sample_875 AS (

  {{ prophecy_basics.Sample('Sort_874', [], 1002, 'firstN', 17) }}

),

RecordID_876 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `MesId`
  
  FROM Sample_875

),

Join_871_inner AS (

  SELECT 
    in0.banco_cobrador AS banco_cobrador,
    in0.cuenta_cobrador AS cuenta_cobrador,
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods_pagador AS idf_pers_ods_pagador,
    in0.importe_arbitrado_dolares AS importe_arbitrado_dolares,
    in0.idf_pers_ods_cobrador AS idf_pers_ods_cobrador,
    in1.MesId AS MesId
  
  FROM Filter_885_reject AS in0
  INNER JOIN RecordID_876 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_881 AS (

  SELECT DISTINCT idf_pers_ods_pagador AS idf_pers_ods
  
  FROM Join_871_inner AS in0

),

AppendFields_882 AS (

  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_876 AS in0
  INNER JOIN Summarize_881 AS in1
     ON true

),

Sort_883 AS (

  SELECT * 
  
  FROM AppendFields_882 AS in0
  
  ORDER BY idf_pers_ods ASC, AAAAMM ASC

),

Summarize_880 AS (

  SELECT 
    COUNT(DISTINCT cuenta_cobrador) AS cantidad_empleados,
    idf_pers_ods_pagador AS idf_pers_ods,
    MesId AS MesId,
    AAAAMM AS AAAAMM
  
  FROM Join_871_inner AS in0
  
  GROUP BY 
    idf_pers_ods_pagador, MesId, AAAAMM

),

Join_877_inner_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE in1.idf_pers_ods
      END
    ) AS idf_pers_ods,
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE in1.MesId
      END
    ) AS MesId,
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE in1.AAAAMM
      END
    ) AS AAAAMM,
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE 0
      END
    ) AS cantidad_empleados
  
  FROM Summarize_880 AS in0
  RIGHT JOIN Sort_883 AS in1
     ON (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))

),

Formula_872_to_Formula_865_0 AS (

  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(AAAAMM AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(cantidad_empleados AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS CantAAAAMM,
    (CONCAT(MesId, 1)) AS MesId,
    * EXCEPT (`mesid`)
  
  FROM Join_877_inner_UnionRightOuter AS in0

),

CrossTab_869 AS (

  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesId,
      CANTAAAAMM
    
    FROM Formula_872_to_Formula_865_0 AS in0
  )
  PIVOT (
    FIRST(CANTAAAAMM) AS First
    FOR MesId
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'
    )
  )

),

DynamicRename_867 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_869'], 
      ['12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'], 
      'editPrefixSuffix', 
      [
        '12', 
        '8', 
        '4', 
        '15', 
        '11', 
        '9', 
        '13', 
        '16', 
        '5', 
        '10', 
        '6', 
        '17', 
        '14', 
        '2', 
        'idf_pers_ods', 
        '18', 
        '7', 
        '3'
      ], 
      'Prefix', 
      'Col', 
      ""
    )
  }}

),

MultiFieldFormula_868 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['DynamicRename_867'], 
      "CASE WHEN (column_value = NULL) THEN ' ' ELSE column_value END", 
      [
        'Col16', 
        'Col4', 
        'Col12', 
        'Col9', 
        'Col8', 
        'Col3', 
        'Col17', 
        'Col10', 
        'Col5', 
        'Col18', 
        'Col6', 
        'Col13', 
        'idf_pers_ods', 
        'Col11', 
        'Col14', 
        'Col15', 
        'Col7', 
        'Col2'
      ], 
      [
        'Col2', 
        'Col3', 
        'Col4', 
        'Col5', 
        'Col6', 
        'Col7', 
        'Col8', 
        'Col9', 
        'Col10', 
        'Col11', 
        'Col12', 
        'Col13', 
        'Col14', 
        'Col15', 
        'Col16', 
        'Col17', 
        'Col18'
      ], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Formula_866_0 AS (

  SELECT 
    18 AS CodProducto,
    '# empleados en otros Bancos' AS Col1,
    *
  
  FROM MultiFieldFormula_868 AS in0

),

Summarize_850 AS (

  SELECT 
    COUNT(DISTINCT idf_pers_ods_cobrador) AS cantidad_empleados,
    idf_pers_ods_pagador AS idf_pers_ods,
    MesId AS MesId,
    AAAAMM AS AAAAMM
  
  FROM Join_852_inner AS in0
  
  GROUP BY 
    idf_pers_ods_pagador, MesId, AAAAMM

),

AppendFields_859 AS (

  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_857 AS in0
  INNER JOIN Summarize_858 AS in1
     ON true

),

Sort_860 AS (

  SELECT * 
  
  FROM AppendFields_859 AS in0
  
  ORDER BY idf_pers_ods ASC, AAAAMM ASC

),

Join_862_inner_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE in1.idf_pers_ods
      END
    ) AS idf_pers_ods,
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE in1.MesId
      END
    ) AS MesId,
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE in1.AAAAMM
      END
    ) AS AAAAMM,
    (
      CASE
        WHEN (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
          THEN NULL
        ELSE 0
      END
    ) AS cantidad_empleados
  
  FROM Summarize_850 AS in0
  RIGHT JOIN Sort_860 AS in1
     ON (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))

),

Formula_853_to_Formula_844_0 AS (

  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(AAAAMM AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(cantidad_empleados AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS CantAAAAMM,
    (CONCAT(MesId, 1)) AS MesId,
    * EXCEPT (`mesid`)
  
  FROM Join_862_inner_UnionRightOuter AS in0

),

CrossTab_849 AS (

  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesId,
      CANTAAAAMM
    
    FROM Formula_853_to_Formula_844_0 AS in0
  )
  PIVOT (
    FIRST(CANTAAAAMM) AS First
    FOR MesId
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'
    )
  )

),

DynamicRename_846 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_849'], 
      ['12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'], 
      'editPrefixSuffix', 
      [
        '12', 
        '8', 
        '4', 
        '15', 
        '11', 
        '9', 
        '13', 
        '16', 
        '5', 
        '10', 
        '6', 
        '17', 
        '14', 
        '2', 
        'idf_pers_ods', 
        '18', 
        '7', 
        '3'
      ], 
      'Prefix', 
      'Col', 
      ""
    )
  }}

),

MultiFieldFormula_848 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['DynamicRename_846'], 
      "CASE WHEN (column_value = NULL) THEN ' ' ELSE column_value END", 
      [
        'Col16', 
        'Col4', 
        'Col12', 
        'Col9', 
        'Col8', 
        'Col3', 
        'Col17', 
        'Col10', 
        'Col5', 
        'Col18', 
        'Col6', 
        'Col13', 
        'idf_pers_ods', 
        'Col11', 
        'Col14', 
        'Col15', 
        'Col7', 
        'Col2'
      ], 
      [
        'Col2', 
        'Col3', 
        'Col4', 
        'Col5', 
        'Col6', 
        'Col7', 
        'Col8', 
        'Col9', 
        'Col10', 
        'Col11', 
        'Col12', 
        'Col13', 
        'Col14', 
        'Col15', 
        'Col16', 
        'Col17', 
        'Col18'
      ], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Formula_845_0 AS (

  SELECT 
    18 AS CodProducto,
    '# empleados en Santander' AS Col1,
    *
  
  FROM MultiFieldFormula_848 AS in0

),

Union_886 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_845_0', 'Formula_866_0'], 
      [
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_578 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6,
    Col7 AS Col7,
    Col8 AS Col8,
    Col9 AS Col9,
    Col10 AS Col10,
    Col11 AS Col11,
    Col12 AS Col12,
    Col13 AS Col13,
    Col14 AS Col14,
    Col15 AS Col15,
    Col16 AS Col16,
    Col17 AS Col17,
    Col18 AS Col18
  
  FROM Union_886 AS in0

),

Union_49_reformat_20 AS (

  SELECT 
    CAST(CodProducto AS STRING) AS CodProducto,
    CAST(Col1 AS STRING) AS Col1,
    CAST(Col10 AS STRING) AS Col10,
    CAST(Col11 AS STRING) AS Col11,
    CAST(Col12 AS STRING) AS Col12,
    CAST(Col13 AS STRING) AS Col13,
    CAST(Col14 AS STRING) AS Col14,
    CAST(Col15 AS STRING) AS Col15,
    CAST(Col16 AS STRING) AS Col16,
    CAST(Col17 AS STRING) AS Col17,
    CAST(Col18 AS STRING) AS Col18,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    CAST(Col4 AS STRING) AS Col4,
    CAST(Col5 AS STRING) AS Col5,
    CAST(Col6 AS STRING) AS Col6,
    CAST(Col7 AS STRING) AS Col7,
    CAST(Col8 AS STRING) AS Col8,
    CAST(Col9 AS STRING) AS Col9
  
  FROM AlteryxSelect_578 AS in0

),

Union_49 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Union_49_reformat_1', 
        'Union_49_reformat_19', 
        'Union_49_reformat_11', 
        'Union_49_reformat_7', 
        'Union_49_reformat_21', 
        'Union_49_reformat_5', 
        'Union_49_reformat_28', 
        'Union_49_reformat_10', 
        'Union_49_reformat_17', 
        'Union_49_reformat_16', 
        'Union_49_reformat_4', 
        'Union_49_reformat_27', 
        'Union_49_reformat_0', 
        'Union_49_reformat_6', 
        'Union_49_reformat_23', 
        'Union_49_reformat_15', 
        'Union_49_reformat_22', 
        'Union_49_reformat_12', 
        'Union_49_reformat_3', 
        'Union_49_reformat_29', 
        'Union_49_reformat_24', 
        'Union_49_reformat_13', 
        'Union_49_reformat_8', 
        'Union_49_reformat_14', 
        'Union_49_reformat_2', 
        'Union_49_reformat_26', 
        'Union_49_reformat_9', 
        'Union_49_reformat_20', 
        'Union_49_reformat_25', 
        'Union_49_reformat_18'
      ], 
      [
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "CodProducto", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_822 AS (

  SELECT * 
  
  FROM Union_49 AS in0
  
  WHERE ((idf_pers_ods IS NOT NULL) AND ((idf_pers_ods <> '9999999999999') OR (idf_pers_ods IS NULL)))

),

AlteryxSelect_50 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    CAST(Col2 AS STRING) AS Col2,
    CAST(Col3 AS STRING) AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6,
    Col7 AS Col7,
    Col8 AS Col8,
    Col9 AS Col9,
    Col10 AS Col10,
    Col11 AS Col11,
    Col12 AS Col12,
    Col13 AS Col13,
    Col14 AS Col14,
    Col15 AS Col15,
    Col16 AS Col16,
    Col17 AS Col17,
    Col18 AS Col18
  
  FROM Filter_822 AS in0

)

SELECT *

FROM AlteryxSelect_50
