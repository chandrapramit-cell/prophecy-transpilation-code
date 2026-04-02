{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH intermediate_productos_aka_Server_UYDB_1034 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1034') }}

),

intermediate_productos_AlteryxSelect_1098 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_AlteryxSelect_1098') }}

),

intermediate_productos_aka_Server_UYDB_1116 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1116') }}

),

Unique_1069 AS (

  {#VisualGroup: Prestamos#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1116 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY codigo_producto, codigo_subproducto, codigo_destino ORDER BY codigo_producto, codigo_subproducto, codigo_destino) = 1

),

intermediate_productos_aka_Server_UYDB_1128 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1128') }}

),

Filter_1083 AS (

  {#VisualGroup: Prestamos#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1128 AS in0
  
  WHERE (perimetro_gestion = CAST('1' AS DOUBLE))

),

Join_1068_inner AS (

  {#VisualGroup: Prestamos#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`codigo_producto`, `codigo_subproducto`, `codigo_destino`)
  
  FROM Filter_1083 AS in0
  INNER JOIN Unique_1069 AS in1
     ON (
      ((in0.codigo_producto = in1.codigo_producto) AND (in0.codigo_subproducto = in1.codigo_subproducto))
      AND (in0.codigo_destino = in1.codigo_destino)
    )

),

intermediate_productos_aka_Server_UYDB_1038 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1038') }}

),

Summarize_1185 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM intermediate_productos_aka_Server_UYDB_1038 AS in0

),

Sample_1188 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  {{ prophecy_basics.Sample('Summarize_1185', [], 1002, 'firstN', 18) }}

),

RecordID_1190 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  {{
    prophecy_basics.RecordID(
      ['Sample_1188'], 
      'incremental_id', 
      'MesID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Join_1204_inner AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`AAAAMM`)
  
  FROM RecordID_1190 AS in0
  INNER JOIN intermediate_productos_aka_Server_UYDB_1038 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_1186 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT 
    COUNT(
      (
        CASE
          WHEN ((idf_pers_ods_cobrador IS NULL) OR (CAST(idf_pers_ods_cobrador AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS cantidad_pagos,
    COUNT(DISTINCT idf_pers_ods_cobrador) AS cantidad_empresas_cobradoras,
    SUM(importe_arbitrado_dolares) AS importe_arbitrado_dolares,
    idf_pers_ods_pagador AS idf_pers_ods,
    MesID AS MesID
  
  FROM Join_1204_inner AS in0
  
  GROUP BY 
    idf_pers_ods_pagador, MesID

),

CrossTab_1189 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesID,
      CANTIDAD_PAGOS
    
    FROM Summarize_1186 AS in0
  )
  PIVOT (
    SUM(CANTIDAD_PAGOS) AS Sum
    FOR MesID
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '1', '17', '14', '2', '18', '7', '3'
    )
  )

),

Formula_1192_0 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT 
    CAST('Fechas' AS string) AS Fechas,
    *
  
  FROM RecordID_1190 AS in0

),

CrossTab_1191 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT *
  
  FROM (
    SELECT 
      MesID,
      AAAAMM
    
    FROM Formula_1192_0 AS in0
  )
  PIVOT (
    FIRST(AAAAMM) AS First
    FOR MesID
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '1', '17', '14', '2', '18', '7', '3'
    )
  )

),

AppendFields_1194 AS (

  {#VisualGroup: PagoProveedoresProd22#}
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
    in0.* EXCEPT (`1`, `2`, `3`, `4`, `5`, `6`, `7`, `8`, `9`, `10`, `11`, `12`, `13`, `14`, `15`, `16`, `17`, `18`),
    in1.*
  
  FROM CrossTab_1191 AS in0
  INNER JOIN CrossTab_1189 AS in1
     ON TRUE

),

Cleanse_1195 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  {{
    prophecy_basics.DataCleansing(
      ['AppendFields_1194'], 
      [
        { "name": "Source_1", "dataType": "String" }, 
        { "name": "Source_2", "dataType": "String" }, 
        { "name": "Source_3", "dataType": "String" }, 
        { "name": "Source_4", "dataType": "String" }, 
        { "name": "Source_5", "dataType": "String" }, 
        { "name": "Source_6", "dataType": "String" }, 
        { "name": "Source_7", "dataType": "String" }, 
        { "name": "Source_8", "dataType": "String" }, 
        { "name": "Source_9", "dataType": "String" }, 
        { "name": "Source_10", "dataType": "String" }, 
        { "name": "Source_11", "dataType": "String" }, 
        { "name": "Source_12", "dataType": "String" }, 
        { "name": "Source_13", "dataType": "String" }, 
        { "name": "Source_14", "dataType": "String" }, 
        { "name": "Source_15", "dataType": "String" }, 
        { "name": "Source_16", "dataType": "String" }, 
        { "name": "Source_17", "dataType": "String" }, 
        { "name": "idf_pers_ods", "dataType": "String" }, 
        { "name": "12", "dataType": "Bigint" }, 
        { "name": "8", "dataType": "Bigint" }, 
        { "name": "4", "dataType": "Bigint" }, 
        { "name": "15", "dataType": "Bigint" }, 
        { "name": "11", "dataType": "Bigint" }, 
        { "name": "9", "dataType": "Bigint" }, 
        { "name": "13", "dataType": "Bigint" }, 
        { "name": "16", "dataType": "Bigint" }, 
        { "name": "5", "dataType": "Bigint" }, 
        { "name": "10", "dataType": "Bigint" }, 
        { "name": "6", "dataType": "Bigint" }, 
        { "name": "1", "dataType": "Bigint" }, 
        { "name": "17", "dataType": "Bigint" }, 
        { "name": "14", "dataType": "Bigint" }, 
        { "name": "2", "dataType": "Bigint" }, 
        { "name": "18", "dataType": "Bigint" }, 
        { "name": "7", "dataType": "Bigint" }, 
        { "name": "3", "dataType": "Bigint" }
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

Formula_1203_0 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT 
    CAST('Cantidad de Pagos' AS string) AS Col1,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_1 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`1` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col2,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_2 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`2` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col3,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_3 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`3` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col4,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_4 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`4` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col5,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_5 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`5` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col6,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_6 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`6` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col7,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_7 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`7` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col8,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_8 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`8` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col9,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_9 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`9` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col10,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_10 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`10` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col11,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_11 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`11` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col12,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_12 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`12` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col13,
    CAST(22 AS INTEGER) AS CodProducto,
    *
  
  FROM Cleanse_1195 AS in0

),

AlteryxSelect_1193 AS (

  {#VisualGroup: PagoProveedoresProd22#}
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
  
  FROM Formula_1203_0 AS in0

),

Filter_1062 AS (

  {#VisualGroup: PrestamoConsumoCodigoProducto17#}
  SELECT * 
  
  FROM Join_1068_inner AS in0
  
  WHERE (
          (
            (
              (
                (NOT ((tipo_producto_comercial = 'Coche')) OR isnull((tipo_producto_comercial = 'Coche')))
                AND (NOT (contains(tipo_producto_comercial, 'GTM')) OR isnull(contains(tipo_producto_comercial, 'GTM')))
              )
              AND (
                    NOT (contains(tipo_producto_comercial, 'Leasing'))
                    OR isnull(contains(tipo_producto_comercial, 'Leasing'))
                  )
            )
            AND (
                  NOT ((grupo_producto_comercial = 'HIpotecarios'))
                  OR isnull((grupo_producto_comercial = 'HIpotecarios'))
                )
          )
          AND (grupo_producto_comercial = 'Consumo')
        )

),

Summarize_1061 AS (

  {#VisualGroup: PrestamoConsumoCodigoProducto17#}
  SELECT DISTINCT idf_pers_ods AS IDF_PERS_ODS
  
  FROM Filter_1062 AS in0

),

Formula_1059_0 AS (

  {#VisualGroup: PrestamoConsumoCodigoProducto17#}
  SELECT 
    CAST(17 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1061 AS in0

),

Union_895_reformat_26 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM Formula_1059_0 AS in0

),

intermediate_productos_aka_Server_UYDB_1032 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1032') }}

),

intermediate_productos_aka_Server_UYDB_1033 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1033') }}

),

Filter_1130 AS (

  {#VisualGroup: FondosdeInversionProd8#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1033 AS in0
  
  WHERE contains(nombre_especie_valor, 'fondo')

),

Join_1133_inner AS (

  {#VisualGroup: FondosdeInversionProd8#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`codigo_valor`)
  
  FROM intermediate_productos_aka_Server_UYDB_1032 AS in0
  INNER JOIN Filter_1130 AS in1
     ON (in0.codigo_valor = in1.codigo_valor)

),

Summarize_1131 AS (

  {#VisualGroup: FondosdeInversionProd8#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Join_1133_inner AS in0

),

Formula_1132_0 AS (

  {#VisualGroup: FondosdeInversionProd8#}
  SELECT 
    CAST(8 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1131 AS in0

),

Union_895_reformat_24 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM Formula_1132_0 AS in0

),

intermediate_productos_aka_Server_UYDB_1037 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1037') }}

),

intermediate_productos_aka_Server_UYDB_1045 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1045') }}

),

Join_1126_inner AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT 
    in1.pan_completo AS pan,
    in0.* EXCEPT (`pan_hash`),
    in1.* EXCEPT (`numero_contrato`, `pan_completo`, `pan_hash`)
  
  FROM intermediate_productos_aka_Server_UYDB_1037 AS in0
  INNER JOIN intermediate_productos_aka_Server_UYDB_1045 AS in1
     ON (in0.pan_hash = in1.pan_hash)

),

Filter_1118 AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT * 
  
  FROM Join_1126_inner AS in0
  
  WHERE (tipo_transaccion = 'Compra TC')

),

Summarize_1120 AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT 
    COUNT(
      (
        CASE
          WHEN ((monto_arbitrado_dolares IS NULL) OR (CAST(monto_arbitrado_dolares AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS cantidad_de_transacciones,
    SUM(monto_arbitrado_dolares) AS monto_arbitrado_dolares,
    AAAAMM AS AAAAMM,
    idf_pers_ods AS idf_pers_ods,
    pan AS pan
  
  FROM Filter_1118 AS in0
  
  GROUP BY 
    AAAAMM, idf_pers_ods, pan

),

Formula_1119_0 AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT 
    CAST(4 AS INTEGER) AS CodProducto,
    CAST((
      CONCAT(
        'PAN: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(pan AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS Col1,
    CAST((
      CONCAT(
        'Mes: ', 
        (
          SUBSTRING(
            CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(AAAAMM AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')) AS string), 
            5, 
            2)
        ), 
        '-', 
        (
          SUBSTRING(
            CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(AAAAMM AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')) AS string), 
            1, 
            4)
        ))
    ) AS string) AS Col2,
    CAST((
      CONCAT(
        'Cantidad Transacciones: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_de_transacciones AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            '')
        ))
    ) AS string) AS Col3,
    CAST((
      CONCAT(
        'Volumen de Transacciones: USD ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(monto_arbitrado_dolares AS DOUBLE), 2)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col4,
    *
  
  FROM Summarize_1120 AS in0

),

CrossTab_1200 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesID,
      IMPORTE_ARBITRADO_DOLARES
    
    FROM Summarize_1186 AS in0
  )
  PIVOT (
    SUM(IMPORTE_ARBITRADO_DOLARES) AS Sum
    FOR MesID
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '1', '17', '14', '2', '18', '7', '3'
    )
  )

),

AppendFields_1197 AS (

  {#VisualGroup: PagoProveedoresProd22#}
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
    in0.* EXCEPT (`1`, `2`, `3`, `4`, `5`, `6`, `7`, `8`, `9`, `10`, `11`, `12`, `13`, `14`, `15`, `16`, `17`, `18`),
    in1.*
  
  FROM CrossTab_1191 AS in0
  INNER JOIN CrossTab_1200 AS in1
     ON TRUE

),

Cleanse_1198 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  {{
    prophecy_basics.DataCleansing(
      ['AppendFields_1197'], 
      [
        { "name": "Source_1", "dataType": "String" }, 
        { "name": "Source_2", "dataType": "String" }, 
        { "name": "Source_3", "dataType": "String" }, 
        { "name": "Source_4", "dataType": "String" }, 
        { "name": "Source_5", "dataType": "String" }, 
        { "name": "Source_6", "dataType": "String" }, 
        { "name": "Source_7", "dataType": "String" }, 
        { "name": "Source_8", "dataType": "String" }, 
        { "name": "Source_9", "dataType": "String" }, 
        { "name": "Source_10", "dataType": "String" }, 
        { "name": "Source_11", "dataType": "String" }, 
        { "name": "Source_12", "dataType": "String" }, 
        { "name": "Source_13", "dataType": "String" }, 
        { "name": "Source_14", "dataType": "String" }, 
        { "name": "Source_15", "dataType": "String" }, 
        { "name": "Source_16", "dataType": "String" }, 
        { "name": "Source_17", "dataType": "String" }, 
        { "name": "idf_pers_ods", "dataType": "String" }, 
        { "name": "12", "dataType": "Decimal" }, 
        { "name": "8", "dataType": "Decimal" }, 
        { "name": "4", "dataType": "Decimal" }, 
        { "name": "15", "dataType": "Decimal" }, 
        { "name": "11", "dataType": "Decimal" }, 
        { "name": "9", "dataType": "Decimal" }, 
        { "name": "13", "dataType": "Decimal" }, 
        { "name": "16", "dataType": "Decimal" }, 
        { "name": "5", "dataType": "Decimal" }, 
        { "name": "10", "dataType": "Decimal" }, 
        { "name": "6", "dataType": "Decimal" }, 
        { "name": "1", "dataType": "Decimal" }, 
        { "name": "17", "dataType": "Decimal" }, 
        { "name": "14", "dataType": "Decimal" }, 
        { "name": "2", "dataType": "Decimal" }, 
        { "name": "18", "dataType": "Decimal" }, 
        { "name": "7", "dataType": "Decimal" }, 
        { "name": "3", "dataType": "Decimal" }
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

intermediate_productos_AlteryxSelect_1288 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_AlteryxSelect_1288') }}

),

Union_895_reformat_22 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col10 AS string) AS Col10,
    CAST(Col11 AS string) AS Col11,
    CAST(Col12 AS string) AS Col12,
    CAST(Col13 AS string) AS Col13,
    CAST(Col14 AS string) AS Col14,
    CAST(Col15 AS string) AS Col15,
    CAST(Col16 AS string) AS Col16,
    CAST(Col17 AS string) AS Col17,
    CAST(Col18 AS string) AS Col18,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6,
    CAST(Col7 AS string) AS Col7,
    CAST(Col8 AS string) AS Col8,
    CAST(Col9 AS string) AS Col9
  
  FROM intermediate_productos_AlteryxSelect_1288 AS in0

),

intermediate_productos_aka_Server_UYDB_1042 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1042') }}

),

Summarize_1206 AS (

  {#VisualGroup: ValoresalCobro#}
  SELECT 
    COUNT(
      (
        CASE
          WHEN ((idf_pers_ods IS NULL) OR (CAST(idf_pers_ods AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS cantidad_operaciones,
    SUM(saldo_arbitrado_dolares) AS saldo_arbitrado_dolares,
    idf_pers_ods AS idf_pers_ods
  
  FROM intermediate_productos_aka_Server_UYDB_1042 AS in0
  
  GROUP BY idf_pers_ods

),

Formula_1207_0 AS (

  {#VisualGroup: ValoresalCobro#}
  SELECT 
    CAST(27 AS INTEGER) AS CodProducto,
    CAST((
      CONCAT(
        'Cantidad operaciones: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_operaciones AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            '')
        ))
    ) AS string) AS Col1,
    CAST((
      CONCAT(
        'Monto a cobrar USD: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(saldo_arbitrado_dolares AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col2,
    *
  
  FROM Summarize_1206 AS in0

),

AlteryxSelect_1208 AS (

  {#VisualGroup: ValoresalCobro#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2
  
  FROM Formula_1207_0 AS in0

),

intermediate_productos_aka_Server_UYDB_1047 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1047') }}

),

Filter_1295 AS (

  {#VisualGroup: GarantiasProd35#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1047 AS in0
  
  WHERE (activa = TRUE)

),

intermediate_productos_aka_Server_UYDB_1048 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1048') }}

),

AlteryxSelect_1300 AS (

  {#VisualGroup: GarantiasProd35#}
  SELECT 
    CAST(numero_garantia AS string) AS numero_garantia,
    * EXCEPT (`numero_garantia`)
  
  FROM intermediate_productos_aka_Server_UYDB_1048 AS in0

),

Join_1296_inner AS (

  {#VisualGroup: GarantiasProd35#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`numero_garantia`, `codigo_relacion`, `numero_persona`, `numero_secuencial_cliente`, `AAAAMM`)
  
  FROM Filter_1295 AS in0
  INNER JOIN AlteryxSelect_1300 AS in1
     ON (in0.numero_garantia = in1.numero_garantia)

),

Summarize_1299 AS (

  {#VisualGroup: GarantiasProd35#}
  SELECT 
    SUM(limite_arbitrado_dolares) AS limite,
    SUM(aplicado_arbitrado_dolares) AS aplicado,
    SUM(disponible_arbitrado_dolares) AS disponible,
    descripcion_estado AS descripcion_estado,
    fecha_vencimiento AS fecha_vencimiento,
    descripcion_corta AS descripcion_corta,
    numero_garantia AS numero_garantia,
    fecha_alta AS fecha_alta,
    idf_pers_ods_garantizado AS idf_pers_ods,
    descripcion_subestado AS descripcion_subestado
  
  FROM Join_1296_inner AS in0
  
  GROUP BY 
    descripcion_estado, 
    fecha_vencimiento, 
    descripcion_corta, 
    numero_garantia, 
    fecha_alta, 
    idf_pers_ods_garantizado, 
    descripcion_subestado

),

intermediate_productos_aka_Server_UYDB_1044 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1044') }}

),

Formula_1199_0 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT 
    CAST('Volumen Arbitrado USD' AS string) AS Col1,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_1 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`1` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col2,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_2 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`2` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col3,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_3 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`3` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col4,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_4 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`4` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col5,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_5 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`5` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col6,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_6 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`6` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col7,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_7 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`7` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col8,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_8 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`8` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col9,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_9 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`9` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col10,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_10 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`10` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col11,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_11 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`11` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col12,
    CAST(concat(
      regexp_replace(regexp_replace(format_number(CAST(Source_12 AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
      ';', 
      regexp_replace(regexp_replace(format_number(CAST(`12` AS DOUBLE), 0), ',', '__THS__'), '__THS__', ',')) AS STRING) AS Col13,
    CAST(22 AS INTEGER) AS CodProducto,
    *
  
  FROM Cleanse_1198 AS in0

),

AlteryxSelect_1196 AS (

  {#VisualGroup: PagoProveedoresProd22#}
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
  
  FROM Formula_1199_0 AS in0

),

Union_1201 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_1193', 'AlteryxSelect_1196'], 
      [
        '[{"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_1202 AS (

  {#VisualGroup: PagoProveedoresProd22#}
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
  
  FROM Union_1201 AS in0

),

Filter_1081 AS (

  {#VisualGroup: PrestamoHipotecarioProd13#}
  SELECT * 
  
  FROM Join_1068_inner AS in0
  
  WHERE (
          (
            (
              (NOT ((tipo_producto_comercial = 'Coche')) OR isnull((tipo_producto_comercial = 'Coche')))
              AND (NOT (contains(tipo_producto_comercial, 'GTM')) OR isnull(contains(tipo_producto_comercial, 'GTM')))
            )
            AND (
                  NOT (contains(tipo_producto_comercial, 'Leasing'))
                  OR isnull(contains(tipo_producto_comercial, 'Leasing'))
                )
          )
          AND (grupo_producto_comercial = 'HIpotecarios')
        )

),

Summarize_1080 AS (

  {#VisualGroup: PrestamoHipotecarioProd13#}
  SELECT DISTINCT idf_pers_ods AS IDF_PERS_ODS
  
  FROM Filter_1081 AS in0

),

Filter_1151 AS (

  {#VisualGroup: Seguros#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1034 AS in0
  
  WHERE ((grupo_producto = 'Vida') AND (tipo_producto = 'Vida Empresarial'))

),

Formula_1152_0 AS (

  {#VisualGroup: Seguros#}
  SELECT 
    CAST(sub_tipo_producto AS string) AS Col1,
    CAST((CONCAT('Plan: ', TRIM(codigo_plan))) AS string) AS Col2,
    CAST((CONCAT('Desde Fecha: ', (DATE_FORMAT(fecha_alta, 'dd-MM-yyyy')))) AS string) AS Col3,
    CAST((
      CONCAT(
        'Cant. Emp.: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_empleados AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS Col4,
    CAST(20 AS INTEGER) AS CodProducto,
    *
  
  FROM Filter_1151 AS in0

),

AlteryxSelect_1154 AS (

  {#VisualGroup: Seguros#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4
  
  FROM Formula_1152_0 AS in0

),

intermediate_productos_aka_Server_UYDB_1040 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1040') }}

),

Filter_1148 AS (

  {#VisualGroup: CuentasActivasProd26#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1040 AS in0
  
  WHERE (
          (
            ((motivo_cancelacion IS NULL) OR ((LENGTH(motivo_cancelacion)) = 0))
            AND ((fecha_cancelacion IS NULL) OR ((LENGTH(CAST(fecha_cancelacion AS string))) = 0))
          )
          AND ((fecha_cierre IS NULL) OR ((LENGTH(CAST(fecha_cierre AS string))) = 0))
        )

),

Summarize_1149 AS (

  {#VisualGroup: CuentasActivasProd26#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Filter_1148 AS in0

),

Formula_1146_0 AS (

  {#VisualGroup: CuentasActivasProd26#}
  SELECT 
    CAST(26 AS INTEGER) AS CodProducto,
    idf_pers_ods AS idf_pers_ods,
    * EXCEPT (`idf_pers_ods`)
  
  FROM Summarize_1149 AS in0

),

Filter_1151_reject AS (

  {#VisualGroup: Seguros#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1034 AS in0
  
  WHERE (
          (NOT((grupo_producto = 'Vida') AND (tipo_producto = 'Vida Empresarial')))
          OR (((grupo_producto = 'Vida') AND (tipo_producto = 'Vida Empresarial')) IS NULL)
        )

),

intermediate_productos_aka_Server_UYDB_1035 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1035') }}

),

Filter_1181 AS (

  {#VisualGroup: DomiciliacionTCCodigoProducto2#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1035 AS in0
  
  WHERE ((tipo_servicio = 'DA - Tarjeta en Cuenta') AND (esta_activo = TRUE))

),

Formula_1182_0 AS (

  {#VisualGroup: DomiciliacionTCCodigoProducto2#}
  SELECT 
    CAST(2 AS string) AS CodProducto,
    CAST(tipo_servicio AS string) AS Col1,
    CAST((CONCAT('Fecha Alta: ', fecha_alta)) AS string) AS Col2,
    CAST((
      CONCAT('Estado: ', (
        CASE
          WHEN (esta_activo = TRUE)
            THEN 'Activo'
          ELSE 'Cancelado'
        END
      ))
    ) AS string) AS Col3,
    CAST((CONCAT('Contrato: ', numero_referencia)) AS string) AS Col4,
    CAST((
      CONCAT(
        'Cuenta Cargo UYU: ', 
        (
          SUBSTRING(
            CAST((SUBSTRING(cargo_cuenta_pesos, 1, 8)) AS string), 
            (((LENGTH(CAST((SUBSTRING(cargo_cuenta_pesos, 1, 8)) AS string))) - 4) + 1), 
            4)
        ), 
        '-', 
        (SUBSTRING(cargo_cuenta_pesos, (((LENGTH(cargo_cuenta_pesos)) - 8) + 1), 8)))
    ) AS string) AS Col5,
    CAST((
      CONCAT(
        'Cuenta Cargo USD: ', 
        (
          SUBSTRING(
            CAST((SUBSTRING(cargo_cuenta_dolares, 1, 8)) AS string), 
            (((LENGTH(CAST((SUBSTRING(cargo_cuenta_dolares, 1, 8)) AS string))) - 4) + 1), 
            4)
        ), 
        '-', 
        (SUBSTRING(cargo_cuenta_dolares, (((LENGTH(cargo_cuenta_dolares)) - 8) + 1), 8)))
    ) AS string) AS Col6,
    *
  
  FROM Filter_1181 AS in0

),

AlteryxSelect_1183 AS (

  {#VisualGroup: DomiciliacionTCCodigoProducto2#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6
  
  FROM Formula_1182_0 AS in0

),

Filter_1051 AS (

  {#VisualGroup: DebitosAutomaticosServiciosenCuentayTCProd3#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1035 AS in0
  
  WHERE (
          (CAST(tipo_servicio AS string) IN ('DA - Servicio en Tarjeta', 'DA - Servicio en Cuenta'))
          AND (esta_activo = TRUE)
        )

),

Formula_1050_0 AS (

  {#VisualGroup: DebitosAutomaticosServiciosenCuentayTCProd3#}
  SELECT 
    CAST(3 AS INTEGER) AS CodProducto,
    CAST(tipo_servicio AS string) AS Col1,
    CAST((CONCAT('Servicio: ', TRIM(nombre_proveedor))) AS string) AS Col2,
    CAST((
      CONCAT(
        'Monto abonado mes USD: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(importe_abonado_arbitrado_dolares AS DOUBLE), 2)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col3,
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
                    CAST((SUBSTRING(cargo_cuenta_pesos, 1, 8)) AS string), 
                    (((LENGTH(CAST((SUBSTRING(cargo_cuenta_pesos, 1, 8)) AS string))) - 4) + 1), 
                    4)
                ), 
                '-', 
                (SUBSTRING(cargo_cuenta_pesos, (((LENGTH(cargo_cuenta_pesos)) - 8) + 1), 8)))
            ) AS string)
          END
        ))
    ) AS string) AS Col4,
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
                    CAST((SUBSTRING(cargo_cuenta_dolares, 1, 8)) AS string), 
                    (((LENGTH(CAST((SUBSTRING(cargo_cuenta_dolares, 1, 8)) AS string))) - 4) + 1), 
                    4)
                ), 
                '-', 
                (SUBSTRING(cargo_cuenta_dolares, (((LENGTH(cargo_cuenta_dolares)) - 8) + 1), 8)))
            ) AS string)
          END
        ))
    ) AS string) AS Col5,
    *
  
  FROM Filter_1051 AS in0

),

AlteryxSelect_1052 AS (

  {#VisualGroup: DebitosAutomaticosServiciosenCuentayTCProd3#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5
  
  FROM Formula_1050_0 AS in0

),

Union_895_reformat_12 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5
  
  FROM AlteryxSelect_1052 AS in0

),

Union_895_reformat_6 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4
  
  FROM AlteryxSelect_1154 AS in0

),

Union_895_reformat_3 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col10 AS string) AS Col10,
    CAST(Col11 AS string) AS Col11,
    CAST(Col12 AS string) AS Col12,
    CAST(Col13 AS string) AS Col13,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6,
    CAST(Col7 AS string) AS Col7,
    CAST(Col8 AS string) AS Col8,
    CAST(Col9 AS string) AS Col9
  
  FROM AlteryxSelect_1202 AS in0

),

intermediate_productos_AlteryxSelect_1241 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_AlteryxSelect_1241') }}

),

Union_895_reformat_18 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5
  
  FROM intermediate_productos_AlteryxSelect_1241 AS in0

),

intermediate_productos_AlteryxSelect_991 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_AlteryxSelect_991') }}

),

Union_895_reformat_17 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6,
    CAST(Col7 AS string) AS Col7,
    CAST(Col8 AS string) AS Col8,
    CAST(Col9 AS string) AS Col9
  
  FROM intermediate_productos_AlteryxSelect_991 AS in0

),

Filter_1057 AS (

  {#VisualGroup: PrestamoCocheProd12#}
  SELECT * 
  
  FROM Join_1068_inner AS in0
  
  WHERE (tipo_producto_comercial = 'Coche')

),

Summarize_1056 AS (

  {#VisualGroup: PrestamoCocheProd12#}
  SELECT DISTINCT idf_pers_ods AS IDF_PERS_ODS
  
  FROM Filter_1057 AS in0

),

Formula_1054_0 AS (

  {#VisualGroup: PrestamoCocheProd12#}
  SELECT 
    CAST(12 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1056 AS in0

),

Union_895_reformat_25 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM Formula_1054_0 AS in0

),

Union_895_reformat_20 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col10 AS string) AS Col10,
    CAST(Col11 AS string) AS Col11,
    CAST(Col12 AS string) AS Col12,
    CAST(Col13 AS string) AS Col13,
    CAST(Col14 AS string) AS Col14,
    CAST(Col15 AS string) AS Col15,
    CAST(Col16 AS string) AS Col16,
    CAST(Col17 AS string) AS Col17,
    CAST(Col18 AS string) AS Col18,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6,
    CAST(Col7 AS string) AS Col7,
    CAST(Col8 AS string) AS Col8,
    CAST(Col9 AS string) AS Col9
  
  FROM intermediate_productos_AlteryxSelect_1098 AS in0

),

Formula_1233_0 AS (

  {#VisualGroup: AcuerdodeSobregiro#}
  SELECT 
    CAST(31 AS INTEGER) AS CodProducto,
    CAST((CONCAT('Cuenta sobregiro: ', codigo_sucursal_operacion, '-', numero_contrato, '-', divisa)) AS string) AS Col1,
    CAST((
      CONCAT(
        'Acuerdo Sobregiro: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(acuerdo_sobregiro_moneda_origen AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col2,
    CAST((
      CONCAT(
        'Sobregiro autorizado tomado: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(sobregiro_autorizado_moneda_origen AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col3,
    CAST((
      CONCAT(
        'Sobregiro autorizado tomado: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(sobregiro_no_autorizado_moneda_origen AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col4,
    CAST((
      CONCAT(
        'Sobregiro disponible: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(sobregiro_disponible_moneda_origen AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col5,
    *
  
  FROM intermediate_productos_aka_Server_UYDB_1044 AS in0

),

AlteryxSelect_1234 AS (

  {#VisualGroup: AcuerdodeSobregiro#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5
  
  FROM Formula_1233_0 AS in0

),

Union_895_reformat_1 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5
  
  FROM AlteryxSelect_1234 AS in0

),

intermediate_productos_Formula_1161 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_Formula_1161') }}

),

Union_895_reformat_15 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col10 AS string) AS Col10,
    CAST(Col11 AS string) AS Col11,
    CAST(Col12 AS string) AS Col12,
    CAST(Col13 AS string) AS Col13,
    CAST(Col14 AS string) AS Col14,
    CAST(Col15 AS string) AS Col15,
    CAST(Col16 AS string) AS Col16,
    CAST(Col17 AS string) AS Col17,
    CAST(Col18 AS string) AS Col18,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6,
    CAST(Col7 AS string) AS Col7,
    CAST(Col8 AS string) AS Col8,
    CAST(Col9 AS string) AS Col9
  
  FROM intermediate_productos_Formula_1161 AS in0

),

Formula_1078_0 AS (

  {#VisualGroup: PrestamoHipotecarioProd13#}
  SELECT 
    CAST(13 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1080 AS in0

),

Union_895_reformat_27 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM Formula_1078_0 AS in0

),

Filter_1144_to_Filter_1143 AS (

  {#VisualGroup: AvalesProd29#}
  SELECT * 
  
  FROM intermediate_productos_aka_Server_UYDB_1128 AS in0
  
  WHERE (
          (
            ((codigo_agrupacion_contable = 'CONTINGENCIAS') AND (codigo_producto = '55'))
            AND (sector_cuenta_contable = '04')
          )
          AND (capitulo_cuenta_contable = '1')
        )

),

Summarize_1142 AS (

  {#VisualGroup: AvalesProd29#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Filter_1144_to_Filter_1143 AS in0

),

Formula_1140_0 AS (

  {#VisualGroup: AvalesProd29#}
  SELECT 
    CAST(29 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1142 AS in0

),

Union_895_reformat_29 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM Formula_1140_0 AS in0

),

Union_895_reformat_2 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2
  
  FROM AlteryxSelect_1208 AS in0

),

Union_895_reformat_30 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM Formula_1146_0 AS in0

),

intermediate_productos_AlteryxSelect_997 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_AlteryxSelect_997') }}

),

Union_895_reformat_14 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6,
    CAST(Col7 AS string) AS Col7,
    CAST(Col8 AS string) AS Col8
  
  FROM intermediate_productos_AlteryxSelect_997 AS in0

),

Formula_1153_0 AS (

  {#VisualGroup: Seguros#}
  SELECT 
    CAST((
      CONCAT(
        grupo_producto, 
        (
          CASE
            WHEN (
              (
                (
                  NOT(
                    grupo_producto = tipo_producto)
                ) OR (grupo_producto IS NULL)
              )
              OR (tipo_producto IS NULL)
            )
              THEN CAST((CONCAT(' - ', tipo_producto)) AS string)
            ELSE ''
          END
        ))
    ) AS string) AS Col1,
    CAST(sub_tipo_producto AS string) AS Col2,
    CAST((CONCAT('Plan: ', TRIM(codigo_plan))) AS string) AS Col3,
    CAST((CONCAT('Desde Fecha: ', (DATE_FORMAT(fecha_alta, 'dd-MM-yyyy')))) AS string) AS Col4,
    CAST((
      CASE
        WHEN (grupo_producto = 'Accidentes Personales')
          THEN 16
        WHEN ((grupo_producto = 'Vida') AND (tipo_producto = 'Vida'))
          THEN 16
        WHEN ((grupo_producto = 'Robo Y Otros') AND (tipo_producto = 'Viaje'))
          THEN 16
        WHEN (grupo_producto = 'Coche')
          THEN 11
        WHEN (grupo_producto = 'Comercio A Primer Riesgo')
          THEN 30
        WHEN CAST((CAST(grupo_producto AS string) IN ('Garantia Alquiler', 'Incendio', 'Vivienda')) AS BOOLEAN)
          THEN 10
        WHEN ((grupo_producto = 'Robo Y Otros') AND (tipo_producto = 'Fraude Full'))
          THEN 6
        ELSE 99
      END
    ) AS INTEGER) AS CodProducto,
    *
  
  FROM Filter_1151_reject AS in0

),

Filter_1156 AS (

  {#VisualGroup: Seguros#}
  SELECT * 
  
  FROM Formula_1153_0 AS in0
  
  WHERE (
          NOT(
            CodProducto = 99)
        )

),

AlteryxSelect_1155 AS (

  {#VisualGroup: Seguros#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4
  
  FROM Filter_1156 AS in0

),

Union_895_reformat_5 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4
  
  FROM AlteryxSelect_1155 AS in0

),

intermediate_productos_aka_Server_UYDB_1039 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_aka_Server_UYDB_1039') }}

),

Summarize_1138 AS (

  {#VisualGroup: DescuentodeChequesProd23#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM intermediate_productos_aka_Server_UYDB_1039 AS in0

),

Formula_1137_0 AS (

  {#VisualGroup: DescuentodeChequesProd23#}
  SELECT 
    CAST(23 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1138 AS in0

),

Union_895_reformat_28 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM Formula_1137_0 AS in0

),

intermediate_productos_AlteryxSelect_1025 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_AlteryxSelect_1025') }}

),

Union_895_reformat_19 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5
  
  FROM intermediate_productos_AlteryxSelect_1025 AS in0

),

Filter_1062_reject_to_Filter_1066 AS (

  {#VisualGroup: PrestamoConsumoCodigoProducto17#}
  SELECT * 
  
  FROM Join_1068_inner AS in0
  
  WHERE (
          (
            (
              (
                (
                  (NOT ((tipo_producto_comercial = 'Coche')) OR isnull((tipo_producto_comercial = 'Coche')))
                  AND (NOT (contains(tipo_producto_comercial, 'GTM')) OR isnull(contains(tipo_producto_comercial, 'GTM')))
                )
                AND (
                      NOT (contains(tipo_producto_comercial, 'Leasing'))
                      OR isnull(contains(tipo_producto_comercial, 'Leasing'))
                    )
              )
              AND (
                    NOT ((grupo_producto_comercial = 'HIpotecarios'))
                    OR isnull((grupo_producto_comercial = 'HIpotecarios'))
                  )
            )
            AND (NOT ((grupo_producto_comercial = 'Consumo')) OR isnull((grupo_producto_comercial = 'Consumo')))
          )
          AND (grupo_producto_comercial = 'Comerciales')
        )

),

Formula_1064_0 AS (

  {#VisualGroup: CreditosComercialesCodigoProducto32#}
  SELECT 
    CAST(32 AS INTEGER) AS CodProducto,
    CAST((
      CONCAT(
        'Destino: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(codigo_destino AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ' ', 
        descripcion_crm)
    ) AS string) AS Col1,
    CAST((
      CONCAT(
        'Monto: ', 
        divisa, 
        ' ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(saldo_moneda_origen AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col2,
    CAST((
      CONCAT(
        'Op.: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(numero_contrato AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS Col3,
    *
  
  FROM Filter_1062_reject_to_Filter_1066 AS in0

),

AlteryxSelect_1065 AS (

  {#VisualGroup: CreditosComercialesCodigoProducto32#}
  SELECT 
    CAST(idf_pers_ods AS string) AS IDF_PERS_ODS,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3
  
  FROM Formula_1064_0 AS in0

),

Union_895_reformat_11 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM AlteryxSelect_1065 AS in0

),

intermediate_productos_AlteryxSelect_994 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_AlteryxSelect_994') }}

),

Union_895_reformat_21 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM intermediate_productos_AlteryxSelect_994 AS in0

),

Filter_1122 AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT * 
  
  FROM Join_1126_inner AS in0
  
  WHERE (tipo_transaccion = 'Compra TD')

),

Summarize_1124 AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT 
    COUNT(
      (
        CASE
          WHEN ((monto_arbitrado_dolares IS NULL) OR (CAST(monto_arbitrado_dolares AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS cantidad_de_transacciones,
    SUM(monto_arbitrado_dolares) AS monto_arbitrado_dolares,
    AAAAMM AS AAAAMM,
    idf_pers_ods AS idf_pers_ods,
    pan AS pan
  
  FROM Filter_1122 AS in0
  
  GROUP BY 
    AAAAMM, idf_pers_ods, pan

),

Formula_1123_0 AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT 
    CAST(5 AS INTEGER) AS CodProducto,
    CAST((
      CONCAT(
        'PAN: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(pan AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS Col1,
    CAST((
      CONCAT(
        'Mes: ', 
        (
          SUBSTRING(
            CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(AAAAMM AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')) AS string), 
            5, 
            2)
        ), 
        '-', 
        (
          SUBSTRING(
            CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(AAAAMM AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')) AS string), 
            1, 
            4)
        ))
    ) AS string) AS Col2,
    CAST((
      CONCAT(
        'Cantidad Transacciones: ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_de_transacciones AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            '')
        ))
    ) AS string) AS Col3,
    CAST((
      CONCAT(
        'Volumen de Transacciones: USD ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(monto_arbitrado_dolares AS DOUBLE), 2)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col4,
    *
  
  FROM Summarize_1124 AS in0

),

AlteryxSelect_1125 AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4
  
  FROM Formula_1123_0 AS in0

),

Union_895_reformat_7 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4
  
  FROM AlteryxSelect_1125 AS in0

),

intermediate_productos_AlteryxSelect_1012 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_AlteryxSelect_1012') }}

),

Union_895_reformat_16 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6,
    CAST(Col7 AS string) AS Col7,
    CAST(Col8 AS string) AS Col8,
    CAST(Col9 AS string) AS Col9
  
  FROM intermediate_productos_AlteryxSelect_1012 AS in0

),

Summarize_1085 AS (

  {#VisualGroup: InversionesProd7#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM intermediate_productos_aka_Server_UYDB_1032 AS in0

),

Formula_1086_0 AS (

  {#VisualGroup: InversionesProd7#}
  SELECT 
    CAST(7 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1085 AS in0

),

Union_895_reformat_23 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM Formula_1086_0 AS in0

),

Union_895_reformat_4 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6
  
  FROM AlteryxSelect_1183 AS in0

),

intermediate_productos_AlteryxSelect_1210 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'intermediate_productos_AlteryxSelect_1210') }}

),

Union_895_reformat_13 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col10 AS string) AS Col10,
    CAST(Col11 AS string) AS Col11,
    CAST(Col12 AS string) AS Col12,
    CAST(Col13 AS string) AS Col13,
    CAST(Col14 AS string) AS Col14,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6,
    CAST(Col7 AS string) AS Col7,
    CAST(Col8 AS string) AS Col8,
    CAST(Col9 AS string) AS Col9
  
  FROM intermediate_productos_AlteryxSelect_1210 AS in0

),

AlteryxSelect_1121 AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4
  
  FROM Formula_1119_0 AS in0

),

Union_895_reformat_8 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4
  
  FROM AlteryxSelect_1121 AS in0

),

Filter_1070 AS (

  {#VisualGroup: CreditoGTMProd25#}
  SELECT * 
  
  FROM Join_1068_inner AS in0
  
  WHERE (
          (NOT ((tipo_producto_comercial = 'Coche')) OR isnull((tipo_producto_comercial = 'Coche')))
          AND contains(tipo_producto_comercial, 'GTM')
        )

),

Formula_1071_0 AS (

  {#VisualGroup: CreditoGTMProd25#}
  SELECT 
    CAST(25 AS INTEGER) AS CodProducto,
    CAST((
      CONCAT(
        'Destino: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(codigo_destino AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ' ', 
        descripcion_crm)
    ) AS string) AS Col1,
    CAST((
      CONCAT(
        'Monto: ', 
        divisa, 
        ' ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(saldo_moneda_origen AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col2,
    CAST((
      CONCAT(
        'Op.: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(numero_contrato AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS Col3,
    *
  
  FROM Filter_1070 AS in0

),

AlteryxSelect_1072 AS (

  {#VisualGroup: CreditoGTMProd25#}
  SELECT 
    CAST(idf_pers_ods AS string) AS IDF_PERS_ODS,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3
  
  FROM Formula_1071_0 AS in0

),

Union_895_reformat_10 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM AlteryxSelect_1072 AS in0

),

Formula_1297_0 AS (

  {#VisualGroup: GarantiasProd35#}
  SELECT 
    CAST(INITCAP(CAST((CONCAT(numero_garantia, ' - ', descripcion_corta)) AS string)) AS string) AS titulo_garantia,
    CAST(35 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1299 AS in0

),

Formula_1297_1 AS (

  {#VisualGroup: GarantiasProd35#}
  SELECT 
    CAST(titulo_garantia AS string) AS Col1,
    CAST((CONCAT('Fecha alta: ', (DATE_FORMAT((TO_DATE(fecha_alta, 'yyyy-MM-dd')), 'dd-MM-yyyy')))) AS string) AS Col2,
    CAST((CONCAT('Fecha vencimiento: ', (DATE_FORMAT((TO_DATE(fecha_vencimiento, 'yyyy-MM-dd')), 'dd-MM-yyyy')))) AS string) AS Col3,
    CAST((CONCAT('Estado: ', descripcion_estado)) AS string) AS Col4,
    CAST((CONCAT('Subestado: ', descripcion_subestado)) AS string) AS Col5,
    CAST((
      CONCAT(
        'Monto limite (Arbitrado USD): ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(limite AS DOUBLE), 2)), ',', '__THS__')), '__THS__', ',')))
    ) AS string) AS Col6,
    CAST((
      CONCAT(
        'Monto aplicado (Arbitrado USD): ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(aplicado AS DOUBLE), 2)), ',', '__THS__')), '__THS__', ',')))
    ) AS string) AS Col7,
    CAST((
      CONCAT(
        'Monto disponible (Arbitrado USD): ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(disponible AS DOUBLE), 2)), ',', '__THS__')), '__THS__', ',')))
    ) AS string) AS Col8,
    *
  
  FROM Formula_1297_0 AS in0

),

AlteryxSelect_1298 AS (

  {#VisualGroup: GarantiasProd35#}
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
    Col8 AS Col8
  
  FROM Formula_1297_1 AS in0

),

Union_895_reformat_0 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5,
    CAST(Col6 AS string) AS Col6,
    CAST(Col7 AS string) AS Col7,
    CAST(Col8 AS string) AS Col8
  
  FROM AlteryxSelect_1298 AS in0

),

Filter_1076 AS (

  {#VisualGroup: LeasingCodigoProducto28#}
  SELECT * 
  
  FROM Join_1068_inner AS in0
  
  WHERE (
          (
            (NOT ((tipo_producto_comercial = 'Coche')) OR isnull((tipo_producto_comercial = 'Coche')))
            AND (NOT (contains(tipo_producto_comercial, 'GTM')) OR isnull(contains(tipo_producto_comercial, 'GTM')))
          )
          AND contains(tipo_producto_comercial, 'Leasing')
        )

),

Formula_1075_0 AS (

  {#VisualGroup: LeasingCodigoProducto28#}
  SELECT 
    CAST(28 AS INTEGER) AS CodProducto,
    CAST((
      CONCAT(
        'Destino: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(codigo_destino AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ' ', 
        descripcion_crm)
    ) AS string) AS Col1,
    CAST((
      CONCAT(
        'Monto: ', 
        divisa, 
        ' ', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(saldo_moneda_origen AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS Col2,
    CAST((
      CONCAT(
        'Op.: ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(numero_contrato AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS Col3,
    *
  
  FROM Filter_1076 AS in0

),

AlteryxSelect_1074 AS (

  {#VisualGroup: LeasingCodigoProducto28#}
  SELECT 
    CAST(idf_pers_ods AS string) AS IDF_PERS_ODS,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3
  
  FROM Formula_1075_0 AS in0

),

Union_895_reformat_9 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM AlteryxSelect_1074 AS in0

),

Union_895 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Union_895_reformat_27', 
        'Union_895_reformat_3', 
        'Union_895_reformat_15', 
        'Union_895_reformat_18', 
        'Union_895_reformat_19', 
        'Union_895_reformat_5', 
        'Union_895_reformat_11', 
        'Union_895_reformat_10', 
        'Union_895_reformat_9', 
        'Union_895_reformat_0', 
        'Union_895_reformat_12', 
        'Union_895_reformat_26', 
        'Union_895_reformat_20', 
        'Union_895_reformat_13', 
        'Union_895_reformat_28', 
        'Union_895_reformat_23', 
        'Union_895_reformat_25', 
        'Union_895_reformat_8', 
        'Union_895_reformat_2', 
        'Union_895_reformat_7', 
        'Union_895_reformat_16', 
        'Union_895_reformat_17', 
        'Union_895_reformat_30', 
        'Union_895_reformat_24', 
        'Union_895_reformat_14', 
        'Union_895_reformat_1', 
        'Union_895_reformat_6', 
        'Union_895_reformat_29', 
        'Union_895_reformat_4', 
        'Union_895_reformat_22', 
        'Union_895_reformat_21'
      ], 
      [
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col16", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col16", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col14", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col16", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col2", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col9", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_895
