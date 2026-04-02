{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH aka_Server_UYDB_1043 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1043') }}

),

Formula_1230_0 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  SELECT 
    CAST(CASE
      WHEN contains(coalesce(lower(datos_adicionales), ''), lower('BPS'))
        THEN 'BPS'
      WHEN contains(coalesce(lower(datos_adicionales), ''), lower('DGI'))
        THEN 'DGI'
      ELSE 'Otros'
    END AS STRING) AS TipoPago,
    *
  
  FROM aka_Server_UYDB_1043 AS in0

),

Summarize_1218 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM Formula_1230_0 AS in0

),

Sample_1220 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  {{ prophecy_basics.Sample('', [], 1002, 'firstN', 13) }}

),

RecordID_1221 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  {{
    prophecy_basics.RecordID(
      ['Sample_1220'], 
      'incremental_id', 
      'MesId', 
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

Join_1222_inner AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  SELECT 
    in0.monto_arbitrado_dolares AS monto_arbitrado_dolares,
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods AS idf_pers_ods,
    in0.TipoPago AS TipoPago,
    in1.MesId AS MesId
  
  FROM Formula_1230_0 AS in0
  INNER JOIN RecordID_1221 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_1223 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT 
    DISTINCT idf_pers_ods AS idf_pers_ods,
    TipoPago AS TipoPago
  
  FROM Join_1222_inner AS in0

),

AppendFields_1224 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in1.TipoPago AS TipoPago,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_1221 AS in0
  INNER JOIN Summarize_1223 AS in1
     ON TRUE

),

Summarize_1217 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  SELECT 
    SUM(monto_arbitrado_dolares) AS monto_arbitrado_dolares,
    idf_pers_ods AS idf_pers_ods,
    TipoPago AS TipoPago,
    AAAAMM AS AAAAMM,
    MesId AS MesId
  
  FROM Join_1222_inner AS in0
  
  GROUP BY 
    idf_pers_ods, TipoPago, AAAAMM, MesId

),

Join_1227_inner_UnionRightOuter AS (

  {#VisualGroup: PagodeImpuestosProd33#}
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
    ) AS MesId,
    in0.* EXCEPT (`monto_arbitrado_dolares`, `AAAAMM`, `idf_pers_ods`, `TipoPago`, `MesId`),
    in1.* EXCEPT (`AAAAMM`, `idf_pers_ods`, `TipoPago`, `MesId`)
  
  FROM Summarize_1217 AS in0
  RIGHT JOIN AppendFields_1224 AS in1
     ON (
      (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))
      AND (in0.TipoPago = in1.TipoPago)
    )

),

Formula_1229_to_Formula_1215_0 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(AAAAMM AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(monto_arbitrado_dolares AS DOUBLE), 2)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS MontoDolaresAAAAMM,
    CAST((CAST(MesId AS INTEGER) + 1) AS INTEGER) AS MesId,
    * EXCEPT (`mesid`)
  
  FROM Join_1227_inner_UnionRightOuter AS in0

),

Filter_1216 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  SELECT * 
  
  FROM Formula_1229_to_Formula_1215_0 AS in0
  
  WHERE (MesId < 15)

),

CrossTab_1211 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      TipoPago,
      MesId,
      MONTODOLARESAAAAMM
    
    FROM Filter_1216 AS in0
  )
  PIVOT (
    FIRST(MONTODOLARESAAAAMM) AS First
    FOR MesId
    IN (
      '12', '8', '4', '11', '9', '13', '5', '10', '6', '14', '2', '7', '3'
    )
  )

),

DynamicRename_1212 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_1211'], 
      ['12', '8', '4', '11', '9', '13', '5', '10', '6', '14', '2', '7', '3'], 
      'editPrefixSuffix', 
      ['12', '8', '4', '11', '9', 'TipoPago', '13', '5', '10', '6', '14', '2', 'idf_pers_ods', '7', '3'], 
      'Prefix', 
      'Col', 
      ""
    )
  }}

),

Formula_1213_0 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
  SELECT 
    CAST(33 AS INTEGER) AS CodProducto,
    CAST(TipoPago AS string) AS Col1,
    *
  
  FROM DynamicRename_1212 AS in0

),

AlteryxSelect_1210 AS (

  {#VisualGroup: PagodeImpuestosProd33#}
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
    * EXCEPT (`TipoPago`, 
    `idf_pers_ods`, 
    `CodProducto`, 
    `Col1`, 
    `Col2`, 
    `Col3`, 
    `Col4`, 
    `Col5`, 
    `Col6`, 
    `Col7`, 
    `Col8`, 
    `Col9`, 
    `Col10`, 
    `Col11`, 
    `Col12`, 
    `Col13`, 
    `Col14`)
  
  FROM Formula_1213_0 AS in0

),

aka_Server_UYDB_1036 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1036') }}

),

Filter_1286_reject AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT * 
  
  FROM aka_Server_UYDB_1036 AS in0
  
  WHERE (
          (
            NOT(
              banco_cobrador = CAST('137' AS INTEGER))
          )
          OR ((banco_cobrador = CAST('137' AS INTEGER)) IS NULL)
        )

),

Summarize_1274 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM Filter_1286_reject AS in0

),

Sample_1276 AS (

  {#VisualGroup: PagodeNominasProd18#}
  {{ prophecy_basics.Sample('', [], 1002, 'firstN', 17) }}

),

RecordID_1277 AS (

  {#VisualGroup: PagodeNominasProd18#}
  {{
    prophecy_basics.RecordID(
      ['Sample_1276'], 
      'incremental_id', 
      'MesId', 
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

Join_1272_inner AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT 
    in0.banco_cobrador AS banco_cobrador,
    in0.cuenta_cobrador AS cuenta_cobrador,
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods_pagador AS idf_pers_ods_pagador,
    in0.importe_arbitrado_dolares AS importe_arbitrado_dolares,
    in0.idf_pers_ods_cobrador AS idf_pers_ods_cobrador,
    in1.MesId AS MesId
  
  FROM Filter_1286_reject AS in0
  INNER JOIN RecordID_1277 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_1282 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT DISTINCT idf_pers_ods_pagador AS idf_pers_ods
  
  FROM Join_1272_inner AS in0

),

AppendFields_1283 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_1277 AS in0
  INNER JOIN Summarize_1282 AS in1
     ON TRUE

),

Summarize_1281 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT 
    COUNT(DISTINCT cuenta_cobrador) AS cantidad_empleados,
    idf_pers_ods_pagador AS idf_pers_ods,
    MesId AS MesId,
    AAAAMM AS AAAAMM
  
  FROM Join_1272_inner AS in0
  
  GROUP BY 
    idf_pers_ods_pagador, MesId, AAAAMM

),

Join_1278_inner_UnionRightOuter AS (

  {#VisualGroup: PagodeNominasProd18#}
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
    ) AS cantidad_empleados,
    in0.* EXCEPT (`idf_pers_ods`, `MesId`, `AAAAMM`, `cantidad_empleados`),
    in1.* EXCEPT (`idf_pers_ods`, `MesId`, `AAAAMM`)
  
  FROM Summarize_1281 AS in0
  RIGHT JOIN AppendFields_1283 AS in1
     ON (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))

),

Formula_1273_to_Formula_1266_0 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(AAAAMM AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_empleados AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS CantAAAAMM,
    CAST((CAST(MesId AS INTEGER) + 1) AS INTEGER) AS MesId,
    * EXCEPT (`mesid`)
  
  FROM Join_1278_inner_UnionRightOuter AS in0

),

aka_Server_UYDB_1046 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1046') }}

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

),

Union_895_reformat_19 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5
  
  FROM AlteryxSelect_1241 AS in0

),

aka_Server_UYDB_1116 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1116') }}

),

Unique_987 AS (

  {#VisualGroup: CalculoProductosMDP#}
  SELECT * 
  
  FROM aka_Server_UYDB_1116 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY codigo_producto, codigo_subproducto ORDER BY codigo_producto, codigo_subproducto) = 1

),

aka_Server_UYDB_1045 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1045') }}

),

aka_Server_UYDB_1031 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1031') }}

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
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1030') }}

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
        { "name": "cantidad_compras_totales_3meses", "dataType": "Double" }, 
        { "name": "tipo_tarjeta", "dataType": "String" }, 
        { "name": "fecha_alta_contrato", "dataType": "Date" }, 
        { "name": "limite_tarjeta", "dataType": "Decimal" }, 
        { "name": "bloqueo_soft_contrato", "dataType": "Boolean" }, 
        { "name": "condicion_economica", "dataType": "String" }, 
        { "name": "tarjeta_transaccional_mes", "dataType": "Boolean" }, 
        { "name": "codigo_producto", "dataType": "String" }, 
        { "name": "pan", "dataType": "String" }, 
        { "name": "bloqueo_duro_tarjeta", "dataType": "Boolean" }
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

),

Filter_989_reject AS (

  {#VisualGroup: CalculoProductosMDP#}
  SELECT * 
  
  FROM Join_988_inner AS in0
  
  WHERE (
          (
            NOT(
              tipo_tarjeta = 'TC')
          ) OR ((tipo_tarjeta = 'TC') IS NULL)
        )

),

Formula_996_0 AS (

  {#VisualGroup: CalculoTDCodigoProducto15#}
  SELECT 
    CAST(15 AS INTEGER) AS CodProducto,
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
        'Tipo tarjeta: ', 
        (
          CASE
            WHEN (es_titular = TRUE)
              THEN 'Titular'
            ELSE 'Adicional'
          END
        ))
    ) AS string) AS Col3,
    CAST((CONCAT('Pan: ', pan)) AS string) AS Col4,
    CAST((CONCAT('Cuenta: ', numero_contrato)) AS string) AS Col5,
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
    ) AS string) AS Col6,
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
    ) AS string) AS Col7,
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
    ) AS string) AS Col8,
    *
  
  FROM Filter_989_reject AS in0

),

Filter_1008 AS (

  {#VisualGroup: CalculoTrilogyCodigoProducto1#}
  SELECT * 
  
  FROM Formula_996_0 AS in0
  
  WHERE ((codigo_producto = '88') AND (CAST(codigo_subproducto AS string) IN ('5601', '7601', '7603')))

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

Unique_1069 AS (

  {#VisualGroup: Prestamos#}
  SELECT * 
  
  FROM aka_Server_UYDB_1116 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY codigo_producto, codigo_subproducto, codigo_destino ORDER BY codigo_producto, codigo_subproducto, codigo_destino) = 1

),

aka_Server_UYDB_1128 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1128') }}

),

Filter_1083 AS (

  {#VisualGroup: Prestamos#}
  SELECT * 
  
  FROM aka_Server_UYDB_1128 AS in0
  
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

AlteryxSelect_997 AS (

  {#VisualGroup: CalculoTDCodigoProducto15#}
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
  
  FROM Formula_996_0 AS in0

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

aka_Server_UYDB_1040 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1040') }}

),

Filter_1148 AS (

  {#VisualGroup: CuentasActivasProd26#}
  SELECT * 
  
  FROM aka_Server_UYDB_1040 AS in0
  
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

aka_Server_UYDB_1038 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1038') }}

),

Summarize_1185 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM aka_Server_UYDB_1038 AS in0

),

Sample_1188 AS (

  {#VisualGroup: PagoProveedoresProd22#}
  {{ prophecy_basics.Sample('', [], 1002, 'firstN', 18) }}

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
  INNER JOIN aka_Server_UYDB_1038 AS in1
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
        { "name": "18", "dataType": "Integer" }, 
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

Filter_1144_to_Filter_1143 AS (

  {#VisualGroup: AvalesProd29#}
  SELECT * 
  
  FROM aka_Server_UYDB_1128 AS in0
  
  WHERE (
          (
            ((codigo_agrupacion_contable = 'CONTINGENCIAS') AND (codigo_producto = '55'))
            AND (sector_cuenta_contable = '04')
          )
          AND (capitulo_cuenta_contable = '1')
        )

),

Summarize_1166 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM aka_Server_UYDB_1036 AS in0

),

Sample_1168 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  {{ prophecy_basics.Sample('', [], 1002, 'firstN', 17) }}

),

RecordID_1169 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  {{
    prophecy_basics.RecordID(
      ['Sample_1168'], 
      'incremental_id', 
      'MesId', 
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

Summarize_1179 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT 
    SUM(importe_arbitrado_dolares) AS importe_arbitrado_dolares,
    AAAAMM AS AAAAMM,
    idf_pers_ods_cobrador AS idf_pers_ods
  
  FROM aka_Server_UYDB_1036 AS in0
  
  GROUP BY 
    AAAAMM, idf_pers_ods_cobrador

),

Join_1170_inner AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT 
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods AS idf_pers_ods,
    in0.importe_arbitrado_dolares AS importe_arbitrado_dolares,
    in1.MesId AS MesId
  
  FROM Summarize_1179 AS in0
  INNER JOIN RecordID_1169 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_1171 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Join_1170_inner AS in0

),

AppendFields_1172 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_1169 AS in0
  INNER JOIN Summarize_1171 AS in1
     ON TRUE

),

Summarize_1165 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT 
    SUM(importe_arbitrado_dolares) AS importe_arbitrado_dolares,
    idf_pers_ods AS idf_pers_ods,
    AAAAMM AS AAAAMM,
    MesId AS MesId
  
  FROM Join_1170_inner AS in0
  
  GROUP BY 
    idf_pers_ods, AAAAMM, MesId

),

Join_1175_inner_UnionRightOuter AS (

  {#VisualGroup: CobrodeSueldosProd9#}
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
    ) AS importe_arbitrado_dolares,
    in0.* EXCEPT (`idf_pers_ods`, `MesId`, `AAAAMM`, `importe_arbitrado_dolares`),
    in1.* EXCEPT (`idf_pers_ods`, `MesId`, `AAAAMM`)
  
  FROM Summarize_1165 AS in0
  RIGHT JOIN AppendFields_1172 AS in1
     ON (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))

),

aka_Server_UYDB_1033 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1033') }}

),

Filter_1286 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT * 
  
  FROM aka_Server_UYDB_1036 AS in0
  
  WHERE (banco_cobrador = CAST('137' AS INTEGER))

),

Summarize_1255 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM Filter_1286 AS in0

),

Sample_1257 AS (

  {#VisualGroup: PagodeNominasProd18#}
  {{ prophecy_basics.Sample('', [], 1002, 'firstN', 17) }}

),

RecordID_1258 AS (

  {#VisualGroup: PagodeNominasProd18#}
  {{
    prophecy_basics.RecordID(
      ['Sample_1257'], 
      'incremental_id', 
      'MesId', 
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

Join_1253_inner AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT 
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods_pagador AS idf_pers_ods_pagador,
    in0.importe_arbitrado_dolares AS importe_arbitrado_dolares,
    in0.idf_pers_ods_cobrador AS idf_pers_ods_cobrador,
    in1.MesId AS MesId
  
  FROM Filter_1286 AS in0
  INNER JOIN RecordID_1258 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

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
        { "name": "18", "dataType": "Double" }, 
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
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]'
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

Union_895_reformat_16 AS (

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

Summarize_1259 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT DISTINCT idf_pers_ods_pagador AS idf_pers_ods
  
  FROM Join_1253_inner AS in0

),

AppendFields_1260 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_1258 AS in0
  INNER JOIN Summarize_1259 AS in1
     ON TRUE

),

Summarize_1251 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT 
    COUNT(DISTINCT idf_pers_ods_cobrador) AS cantidad_empleados,
    idf_pers_ods_pagador AS idf_pers_ods,
    MesId AS MesId,
    AAAAMM AS AAAAMM
  
  FROM Join_1253_inner AS in0
  
  GROUP BY 
    idf_pers_ods_pagador, MesId, AAAAMM

),

Join_1263_inner_UnionRightOuter AS (

  {#VisualGroup: PagodeNominasProd18#}
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
    ) AS cantidad_empleados,
    in0.* EXCEPT (`idf_pers_ods`, `MesId`, `AAAAMM`, `cantidad_empleados`),
    in1.* EXCEPT (`idf_pers_ods`, `MesId`, `AAAAMM`)
  
  FROM Summarize_1251 AS in0
  RIGHT JOIN AppendFields_1260 AS in1
     ON (((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.MesId = in1.MesId)) AND (in0.AAAAMM = in1.AAAAMM))

),

Formula_1254_to_Formula_1246_0 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(AAAAMM AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(cantidad_empleados AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS CantAAAAMM,
    CAST((CAST(MesId AS INTEGER) + 1) AS INTEGER) AS MesId,
    * EXCEPT (`mesid`)
  
  FROM Join_1263_inner_UnionRightOuter AS in0

),

CrossTab_1250 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesId,
      CANTAAAAMM
    
    FROM Formula_1254_to_Formula_1246_0 AS in0
  )
  PIVOT (
    FIRST(CANTAAAAMM) AS First
    FOR MesId
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'
    )
  )

),

DynamicRename_1248 AS (

  {#VisualGroup: PagodeNominasProd18#}
  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_1250'], 
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

MultiFieldFormula_1249 AS (

  {#VisualGroup: PagodeNominasProd18#}
  {{
    prophecy_basics.MultiColumnEdit(
      ['DynamicRename_1248'], 
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

aka_Server_UYDB_1044 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1044') }}

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
  
  FROM aka_Server_UYDB_1044 AS in0

),

Filter_989 AS (

  {#VisualGroup: CalculoProductosMDP#}
  SELECT * 
  
  FROM Join_988_inner AS in0
  
  WHERE (tipo_tarjeta = 'TC')

),

Formula_992_0 AS (

  {#VisualGroup: CalculoTCCodigoProducto1436373839#}
  SELECT 
    CAST(CASE
      WHEN ((codigo_producto = '77') AND contains(coalesce(lower(upper(descripcion_crm)), ''), lower('SOY')))
        THEN 37
      WHEN (
        (codigo_producto = '77')
        AND NOT (contains(coalesce(lower(upper(descripcion_crm)), ''), lower('SOY')))
      )
        THEN 36
      WHEN ((codigo_producto = '80') AND contains(coalesce(lower(upper(descripcion_crm)), ''), lower('SOY')))
        THEN 39
      WHEN (
        (codigo_producto = '80')
        AND NOT (contains(coalesce(lower(upper(descripcion_crm)), ''), lower('SOY')))
      )
        THEN 38
      ELSE 99
    END AS INT) AS CodProducto,
    *
  
  FROM Filter_989 AS in0

),

Filter_993 AS (

  {#VisualGroup: CalculoTCCodigoProducto1436373839#}
  SELECT * 
  
  FROM Formula_992_0 AS in0
  
  WHERE (
          NOT(
            CodProducto = 99)
        )

),

aka_Server_UYDB_1042 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1042') }}

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
  
  FROM aka_Server_UYDB_1042 AS in0
  
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

aka_Server_UYDB_1032 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1032') }}

),

Filter_1130 AS (

  {#VisualGroup: FondosdeInversionProd8#}
  SELECT * 
  
  FROM aka_Server_UYDB_1033 AS in0
  
  WHERE contains(nombre_especie_valor, 'fondo')

),

Join_1133_inner AS (

  {#VisualGroup: FondosdeInversionProd8#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`codigo_valor`)
  
  FROM aka_Server_UYDB_1032 AS in0
  INNER JOIN Filter_1130 AS in1
     ON (in0.codigo_valor = in1.codigo_valor)

),

Summarize_1131 AS (

  {#VisualGroup: FondosdeInversionProd8#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Join_1133_inner AS in0

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

aka_Server_UYDB_1047 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1047') }}

),

Filter_1295 AS (

  {#VisualGroup: GarantiasProd35#}
  SELECT * 
  
  FROM aka_Server_UYDB_1047 AS in0
  
  WHERE (activa = TRUE)

),

aka_Server_UYDB_1048 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1048') }}

),

AlteryxSelect_1300 AS (

  {#VisualGroup: GarantiasProd35#}
  SELECT 
    CAST(numero_garantia AS string) AS numero_garantia,
    * EXCEPT (`numero_garantia`)
  
  FROM aka_Server_UYDB_1048 AS in0

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
        '[{"name": "idf_pers_ods_tarjeta", "dataType": "String"}, {"name": "motivo_baja_contrato", "dataType": "String"}, {"name": "numero_contrato", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "es_titular", "dataType": "Boolean"}, {"name": "bloqueo_soft_tarjeta", "dataType": "Boolean"}, {"name": "Col9", "dataType": "String"}, {"name": "idf_pers_ods_contrato", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "bloqueo_duro_contrato", "dataType": "Boolean"}, {"name": "cantidad_compras_totales", "dataType": "Integer"}, {"name": "fecha_baja_contrato", "dataType": "Date"}, {"name": "codigo_subproducto", "dataType": "String"}, {"name": "codigo_bloqueo_contrato", "dataType": "Double"}, {"name": "cantidad_compras_totales_3meses", "dataType": "Double"}, {"name": "tipo_tarjeta", "dataType": "String"}, {"name": "fecha_alta_contrato", "dataType": "Date"}, {"name": "limite_tarjeta", "dataType": "Decimal"}, {"name": "Col5", "dataType": "String"}, {"name": "bloqueo_soft_contrato", "dataType": "Boolean"}, {"name": "condicion_economica", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "tarjeta_transaccional_mes", "dataType": "Boolean"}, {"name": "codigo_destino", "dataType": "String"}, {"name": "descripcion_crm", "dataType": "String"}, {"name": "codigo_producto", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "pan", "dataType": "String"}, {"name": "sub_tipo_producto_comercial", "dataType": "String"}, {"name": "bloqueo_duro_tarjeta", "dataType": "Boolean"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "idf_pers_ods_tarjeta", "dataType": "String"}, {"name": "motivo_baja_contrato", "dataType": "String"}, {"name": "numero_contrato", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "es_titular", "dataType": "Boolean"}, {"name": "bloqueo_soft_tarjeta", "dataType": "Boolean"}, {"name": "idf_pers_ods_contrato", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "bloqueo_duro_contrato", "dataType": "Boolean"}, {"name": "cantidad_compras_totales", "dataType": "Integer"}, {"name": "fecha_baja_contrato", "dataType": "Date"}, {"name": "codigo_subproducto", "dataType": "String"}, {"name": "codigo_bloqueo_contrato", "dataType": "Double"}, {"name": "cantidad_compras_totales_3meses", "dataType": "Double"}, {"name": "tipo_tarjeta", "dataType": "String"}, {"name": "fecha_alta_contrato", "dataType": "Date"}, {"name": "limite_tarjeta", "dataType": "Decimal"}, {"name": "Col5", "dataType": "String"}, {"name": "bloqueo_soft_contrato", "dataType": "Boolean"}, {"name": "condicion_economica", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "tarjeta_transaccional_mes", "dataType": "Boolean"}, {"name": "codigo_destino", "dataType": "String"}, {"name": "descripcion_crm", "dataType": "String"}, {"name": "codigo_producto", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "pan", "dataType": "String"}, {"name": "sub_tipo_producto_comercial", "dataType": "String"}, {"name": "bloqueo_duro_tarjeta", "dataType": "Boolean"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]'
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

),

Union_895_reformat_3 AS (

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
  
  FROM AlteryxSelect_1012 AS in0

),

aka_Server_UYDB_1041 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1041') }}

),

Summarize_1102 AS (

  {#VisualGroup: TPVProd19#}
  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM aka_Server_UYDB_1041 AS in0

),

Sample_1104 AS (

  {#VisualGroup: TPVProd19#}
  {{ prophecy_basics.Sample('', [], 1002, 'firstN', 17) }}

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

Formula_1177_to_Formula_1163_0 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(AAAAMM AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(importe_arbitrado_dolares AS DOUBLE), 2)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS MontoDolaresAAAAMM,
    CAST((CAST(MesId AS INTEGER) + 1) AS INTEGER) AS MesId,
    * EXCEPT (`mesid`)
  
  FROM Join_1175_inner_UnionRightOuter AS in0

),

Filter_1164 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT * 
  
  FROM Formula_1177_to_Formula_1163_0 AS in0
  
  WHERE (MesId < 19)

),

CrossTab_1159 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesId,
      MONTODOLARESAAAAMM
    
    FROM Filter_1164 AS in0
  )
  PIVOT (
    CONCAT_WS(', ', COLLECT_LIST(MONTODOLARESAAAAMM)) AS Concat
    FOR MesId
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'
    )
  )

),

DynamicRename_1160 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_1159'], 
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

Union_895_reformat_30 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM Formula_1146_0 AS in0

),

aka_Server_UYDB_1034 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1034') }}

),

Filter_1151 AS (

  {#VisualGroup: Seguros#}
  SELECT * 
  
  FROM aka_Server_UYDB_1034 AS in0
  
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

aka_Server_UYDB_1037 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1037') }}

),

Join_1126_inner AS (

  {#VisualGroup: ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5#}
  SELECT 
    in1.pan_completo AS pan,
    in0.* EXCEPT (`pan_hash`),
    in1.* EXCEPT (`numero_contrato`, `pan_completo`, `pan_hash`)
  
  FROM aka_Server_UYDB_1037 AS in0
  INNER JOIN aka_Server_UYDB_1045 AS in1
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

aka_Server_UYDB_1035 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1035') }}

),

aka_Server_UYDB_1039 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('right_s_1', 'aka_Server_UYDB_1039') }}

),

Summarize_1138 AS (

  {#VisualGroup: DescuentodeChequesProd23#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM aka_Server_UYDB_1039 AS in0

),

Formula_1137_0 AS (

  {#VisualGroup: DescuentodeChequesProd23#}
  SELECT 
    CAST(23 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1138 AS in0

),

Filter_1151_reject AS (

  {#VisualGroup: Seguros#}
  SELECT * 
  
  FROM aka_Server_UYDB_1034 AS in0
  
  WHERE (
          (NOT((grupo_producto = 'Vida') AND (tipo_producto = 'Vida Empresarial')))
          OR (((grupo_producto = 'Vida') AND (tipo_producto = 'Vida Empresarial')) IS NULL)
        )

),

RecordID_1105 AS (

  {#VisualGroup: TPVProd19#}
  {{
    prophecy_basics.RecordID(
      ['Sample_1104'], 
      'incremental_id', 
      'MesId', 
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

Join_1101_inner AS (

  {#VisualGroup: TPVProd19#}
  SELECT 
    in0.monto_arbitrado_dolares AS monto_arbitrado_dolares,
    in0.AAAAMM AS AAAAMM,
    in0.idf_pers_ods AS idf_pers_ods,
    in0.merchant AS merchant,
    in1.MesId AS MesId
  
  FROM aka_Server_UYDB_1041 AS in0
  INNER JOIN RecordID_1105 AS in1
     ON (in0.AAAAMM = in1.AAAAMM)

),

Summarize_1106 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM Join_1101_inner AS in0

),

Summarize_1111 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT DISTINCT merchant AS merchant
  
  FROM aka_Server_UYDB_1041 AS in0

),

AppendFields_1109 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_1111 AS in0
  INNER JOIN RecordID_1105 AS in1
     ON TRUE

),

AppendFields_1107 AS (

  {#VisualGroup: Estoesparaponertodoslosmesesaunqueesteen0#}
  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.AAAAMM AS AAAAMM,
    in0.MesId AS MesId,
    in0.merchant AS merchant
  
  FROM AppendFields_1109 AS in0
  INNER JOIN Summarize_1106 AS in1
     ON TRUE

),

Summarize_1097 AS (

  {#VisualGroup: TPVProd19#}
  SELECT 
    SUM(monto_arbitrado_dolares) AS monto_arbitrado_dolares,
    merchant AS merchant,
    idf_pers_ods AS idf_pers_ods,
    AAAAMM AS AAAAMM,
    MesID AS MesID
  
  FROM Join_1101_inner AS in0
  
  GROUP BY 
    merchant, idf_pers_ods, AAAAMM, MesId

),

Join_1089_inner_UnionRightOuter AS (

  {#VisualGroup: TPVProd19#}
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
    in0.* EXCEPT (`monto_arbitrado_dolares`, `AAAAMM`, `idf_pers_ods`, `merchant`, `MesId`),
    in1.* EXCEPT (`AAAAMM`, `idf_pers_ods`, `merchant`)
  
  FROM Summarize_1097 AS in0
  RIGHT JOIN AppendFields_1107 AS in1
     ON (
      ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.AAAAMM = in1.AAAAMM))
      AND (in0.merchant = in1.merchant)
    )

),

MultiFieldFormula_1099 AS (

  {#VisualGroup: TPVProd19#}
  {{
    prophecy_basics.MultiColumnEdit(
      ['Join_1089_inner_UnionRightOuter'], 
      "CASE WHEN (column_value = NULL) THEN CASE WHEN (column_value = 0) THEN -1 ELSE 0 END ELSE column_value END", 
      ['MesID', 'monto_arbitrado_dolares', 'idf_pers_ods', 'merchant', 'AAAAMM'], 
      ['monto_arbitrado_dolares'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Formula_1091_to_Formula_1095_0 AS (

  {#VisualGroup: TPVProd19#}
  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(AAAAMM AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ';', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((FORMAT_NUMBER(CAST(monto_arbitrado_dolares AS DOUBLE), 0)), ',', '__THS__')), 
            '__THS__', 
            ',')
        ))
    ) AS string) AS MontoDolaresAAAAMM,
    CAST((MesID + 1) AS INTEGER) AS MesID,
    * EXCEPT (`mesid`)
  
  FROM MultiFieldFormula_1099 AS in0

),

Filter_1096 AS (

  {#VisualGroup: TPVProd19#}
  SELECT * 
  
  FROM Formula_1091_to_Formula_1095_0 AS in0
  
  WHERE (MesID < 19)

),

CrossTab_1092 AS (

  {#VisualGroup: TPVProd19#}
  SELECT *
  
  FROM (
    SELECT 
      merchant,
      idf_pers_ods,
      MesID,
      MONTODOLARESAAAAMM
    
    FROM Filter_1096 AS in0
  )
  PIVOT (
    FIRST(MONTODOLARESAAAAMM) AS First
    FOR MesID
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'
    )
  )

),

DynamicRename_1093 AS (

  {#VisualGroup: TPVProd19#}
  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_1092'], 
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

Filter_1181 AS (

  {#VisualGroup: DomiciliacionTCCodigoProducto2#}
  SELECT * 
  
  FROM aka_Server_UYDB_1035 AS in0
  
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

Union_895_reformat_2 AS (

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
  
  FROM AlteryxSelect_997 AS in0

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

Union_895_reformat_21 AS (

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

Union_895_reformat_6 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM AlteryxSelect_1065 AS in0

),

Union_895_reformat_26 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM Formula_1059_0 AS in0

),

Summarize_1085 AS (

  {#VisualGroup: InversionesProd7#}
  SELECT DISTINCT idf_pers_ods AS idf_pers_ods
  
  FROM aka_Server_UYDB_1032 AS in0

),

Formula_1086_0 AS (

  {#VisualGroup: InversionesProd7#}
  SELECT 
    CAST(7 AS INTEGER) AS CodProducto,
    *
  
  FROM Summarize_1085 AS in0

),

Union_895_reformat_22 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM Formula_1086_0 AS in0

),

AlteryxSelect_991 AS (

  {#VisualGroup: CalculoTCCodigoProducto1436373839#}
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
  
  FROM Formula_990_0 AS in0

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
    CAST(Col8 AS string) AS Col8,
    CAST(Col9 AS string) AS Col9
  
  FROM AlteryxSelect_991 AS in0

),

Union_895_reformat_18 AS (

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
  
  FROM AlteryxSelect_1210 AS in0

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

Union_895_reformat_8 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM AlteryxSelect_1074 AS in0

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

Union_895_reformat_11 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4
  
  FROM AlteryxSelect_1125 AS in0

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

Union_895_reformat_10 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4
  
  FROM AlteryxSelect_1121 AS in0

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

),

Union_895_reformat_4 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5
  
  FROM AlteryxSelect_1025 AS in0

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

Union_895_reformat_13 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4
  
  FROM AlteryxSelect_1155 AS in0

),

AlteryxSelect_994 AS (

  {#VisualGroup: CalculoTCCodigoProducto1436373839#}
  SELECT 
    idf_pers_ods_tarjeta AS idf_pers_ods,
    CodProducto AS CodProducto
  
  FROM Filter_993 AS in0

),

Union_895_reformat_1 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM AlteryxSelect_994 AS in0

),

Union_895_reformat_15 AS (

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

Formula_1247_0 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT 
    CAST(18 AS INTEGER) AS CodProducto,
    CAST('# empleados en Santander' AS string) AS Col1,
    *
  
  FROM MultiFieldFormula_1249 AS in0

),

CrossTab_1270 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      MesId,
      CANTAAAAMM
    
    FROM Formula_1273_to_Formula_1266_0 AS in0
  )
  PIVOT (
    FIRST(CANTAAAAMM) AS First
    FOR MesId
    IN (
      '12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'
    )
  )

),

DynamicRename_1268 AS (

  {#VisualGroup: PagodeNominasProd18#}
  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_1270'], 
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

MultiFieldFormula_1269 AS (

  {#VisualGroup: PagodeNominasProd18#}
  {{
    prophecy_basics.MultiColumnEdit(
      ['DynamicRename_1268'], 
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

Formula_1267_0 AS (

  {#VisualGroup: PagodeNominasProd18#}
  SELECT 
    CAST(18 AS INTEGER) AS CodProducto,
    CAST('# empleados en otros Bancos' AS string) AS Col1,
    *
  
  FROM MultiFieldFormula_1269 AS in0

),

Union_1287 AS (

  {#VisualGroup: PagodeNominasProd18#}
  {{
    prophecy_basics.UnionByName(
      ['Formula_1247_0', 'Formula_1267_0'], 
      [
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_1288 AS (

  {#VisualGroup: PagodeNominasProd18#}
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
  
  FROM Union_1287 AS in0

),

Union_895_reformat_14 AS (

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
  
  FROM AlteryxSelect_1288 AS in0

),

Formula_1094_0 AS (

  {#VisualGroup: TPVProd19#}
  SELECT 
    CAST(19 AS INTEGER) AS CodProducto,
    CAST((CONCAT('Importe Cobrado ', merchant)) AS string) AS Col1,
    *
  
  FROM DynamicRename_1093 AS in0

),

Filter_1114 AS (

  {#VisualGroup: TPVProd19#}
  SELECT * 
  
  FROM Formula_1094_0 AS in0
  
  WHERE (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            NOT(
                                                                              CAST((SUBSTRING(Col2, (((LENGTH(Col2)) - 2) + 1), 2)) AS string) = ';0')
                                                                          )
                                                                          OR ((SUBSTRING(Col2, (((LENGTH(Col2)) - 2) + 1), 2)) IS NULL)
                                                                        )
                                                                        OR (
                                                                             NOT(
                                                                               CAST((SUBSTRING(Col3, (((LENGTH(Col3)) - 2) + 1), 2)) AS string) = ';0')
                                                                           )
                                                                      )
                                                                      OR ((SUBSTRING(Col3, (((LENGTH(Col3)) - 2) + 1), 2)) IS NULL)
                                                                    )
                                                                    OR (
                                                                         NOT(
                                                                           CAST((SUBSTRING(Col4, (((LENGTH(Col4)) - 2) + 1), 2)) AS string) = ';0')
                                                                       )
                                                                  )
                                                                  OR ((SUBSTRING(Col4, (((LENGTH(Col4)) - 2) + 1), 2)) IS NULL)
                                                                )
                                                                OR (
                                                                     NOT(
                                                                       CAST((SUBSTRING(Col5, (((LENGTH(Col5)) - 2) + 1), 2)) AS string) = ';0')
                                                                   )
                                                              )
                                                              OR ((SUBSTRING(Col5, (((LENGTH(Col5)) - 2) + 1), 2)) IS NULL)
                                                            )
                                                            OR (
                                                                 NOT(
                                                                   CAST((SUBSTRING(Col6, (((LENGTH(Col6)) - 2) + 1), 2)) AS string) = ';0')
                                                               )
                                                          )
                                                          OR ((SUBSTRING(Col6, (((LENGTH(Col6)) - 2) + 1), 2)) IS NULL)
                                                        )
                                                        OR (
                                                             NOT(
                                                               CAST((SUBSTRING(Col7, (((LENGTH(Col7)) - 2) + 1), 2)) AS string) = ';0')
                                                           )
                                                      )
                                                      OR ((SUBSTRING(Col7, (((LENGTH(Col7)) - 2) + 1), 2)) IS NULL)
                                                    )
                                                    OR (
                                                         NOT(
                                                           CAST((SUBSTRING(Col8, (((LENGTH(Col8)) - 2) + 1), 2)) AS string) = ';0')
                                                       )
                                                  )
                                                  OR ((SUBSTRING(Col8, (((LENGTH(Col8)) - 2) + 1), 2)) IS NULL)
                                                )
                                                OR (
                                                     NOT(
                                                       CAST((SUBSTRING(Col9, (((LENGTH(Col9)) - 2) + 1), 2)) AS string) = ';0')
                                                   )
                                              )
                                              OR ((SUBSTRING(Col9, (((LENGTH(Col9)) - 2) + 1), 2)) IS NULL)
                                            )
                                            OR (
                                                 NOT(
                                                   CAST((SUBSTRING(Col10, (((LENGTH(Col10)) - 2) + 1), 2)) AS string) = ';0')
                                               )
                                          )
                                          OR ((SUBSTRING(Col10, (((LENGTH(Col10)) - 2) + 1), 2)) IS NULL)
                                        )
                                        OR (
                                             NOT(
                                               CAST((SUBSTRING(Col11, (((LENGTH(Col11)) - 2) + 1), 2)) AS string) = ';0')
                                           )
                                      )
                                      OR ((SUBSTRING(Col11, (((LENGTH(Col11)) - 2) + 1), 2)) IS NULL)
                                    )
                                    OR (
                                         NOT(
                                           CAST((SUBSTRING(Col12, (((LENGTH(Col12)) - 2) + 1), 2)) AS string) = ';0')
                                       )
                                  )
                                  OR ((SUBSTRING(Col12, (((LENGTH(Col12)) - 2) + 1), 2)) IS NULL)
                                )
                                OR (
                                     NOT(
                                       CAST((SUBSTRING(Col13, (((LENGTH(Col13)) - 2) + 1), 2)) AS string) = ';0')
                                   )
                              )
                              OR ((SUBSTRING(Col13, (((LENGTH(Col13)) - 2) + 1), 2)) IS NULL)
                            )
                            OR (
                                 NOT(
                                   CAST((SUBSTRING(Col14, (((LENGTH(Col14)) - 2) + 1), 2)) AS string) = ';0')
                               )
                          )
                          OR ((SUBSTRING(Col14, (((LENGTH(Col14)) - 2) + 1), 2)) IS NULL)
                        )
                        OR (
                             NOT(
                               CAST((SUBSTRING(Col15, (((LENGTH(Col15)) - 2) + 1), 2)) AS string) = ';0')
                           )
                      )
                      OR ((SUBSTRING(Col15, (((LENGTH(Col15)) - 2) + 1), 2)) IS NULL)
                    )
                    OR (
                         NOT(
                           CAST((SUBSTRING(Col16, (((LENGTH(Col16)) - 2) + 1), 2)) AS string) = ';0')
                       )
                  )
                  OR ((SUBSTRING(Col16, (((LENGTH(Col16)) - 2) + 1), 2)) IS NULL)
                )
                OR (
                     NOT(
                       CAST((SUBSTRING(Col17, (((LENGTH(Col17)) - 2) + 1), 2)) AS string) = ';0')
                   )
              )
              OR ((SUBSTRING(Col17, (((LENGTH(Col17)) - 2) + 1), 2)) IS NULL)
            )
            OR (
                 NOT(
                   CAST((SUBSTRING(Col18, (((LENGTH(Col18)) - 2) + 1), 2)) AS string) = ';0')
               )
          )
          OR ((SUBSTRING(Col18, (((LENGTH(Col18)) - 2) + 1), 2)) IS NULL)
        )

),

AlteryxSelect_1098 AS (

  {#VisualGroup: TPVProd19#}
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
  
  FROM Filter_1114 AS in0

),

Union_895_reformat_9 AS (

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
  
  FROM AlteryxSelect_1098 AS in0

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

Union_895_reformat_20 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5
  
  FROM AlteryxSelect_1234 AS in0

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

Union_895_reformat_7 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    IDF_PERS_ODS AS IDF_PERS_ODS
  
  FROM AlteryxSelect_1072 AS in0

),

Formula_1161_0 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT 
    CAST(9 AS INTEGER) AS CodProducto,
    CAST('Importe Cobrado' AS string) AS Col1,
    *
  
  FROM DynamicRename_1160 AS in0

),

Union_895_reformat_23 AS (

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
  
  FROM Formula_1161_0 AS in0

),

Union_895_reformat_28 AS (

  SELECT CAST(CodProducto AS string) AS CodProducto
  
  FROM Formula_1137_0 AS in0

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

Filter_1051 AS (

  {#VisualGroup: DebitosAutomaticosServiciosenCuentayTCProd3#}
  SELECT * 
  
  FROM aka_Server_UYDB_1035 AS in0
  
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

Union_895_reformat_5 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4,
    CAST(Col5 AS string) AS Col5
  
  FROM AlteryxSelect_1052 AS in0

),

Union_895_reformat_17 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2
  
  FROM AlteryxSelect_1208 AS in0

),

Union_895_reformat_12 AS (

  SELECT 
    CAST(CodProducto AS string) AS CodProducto,
    CAST(Col1 AS string) AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    CAST(Col4 AS string) AS Col4
  
  FROM AlteryxSelect_1154 AS in0

),

Union_895 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Union_895_reformat_27', 
        'Union_895_reformat_16', 
        'Union_895_reformat_23', 
        'Union_895_reformat_19', 
        'Union_895_reformat_4', 
        'Union_895_reformat_13', 
        'Union_895_reformat_6', 
        'Union_895_reformat_7', 
        'Union_895_reformat_8', 
        'Union_895_reformat_21', 
        'Union_895_reformat_5', 
        'Union_895_reformat_26', 
        'Union_895_reformat_9', 
        'Union_895_reformat_18', 
        'Union_895_reformat_28', 
        'Union_895_reformat_22', 
        'Union_895_reformat_25', 
        'Union_895_reformat_10', 
        'Union_895_reformat_17', 
        'Union_895_reformat_11', 
        'Union_895_reformat_3', 
        'Union_895_reformat_0', 
        'Union_895_reformat_30', 
        'Union_895_reformat_24', 
        'Union_895_reformat_2', 
        'Union_895_reformat_20', 
        'Union_895_reformat_12', 
        'Union_895_reformat_29', 
        'Union_895_reformat_15', 
        'Union_895_reformat_14', 
        'Union_895_reformat_1'
      ], 
      [
        '[{"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "IDF_PERS_ODS", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}, {"name": "Col1", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]', 
        '[{"name": "Col4", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "CodProducto", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col1", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "CodProducto", "dataType": "Integer"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_895
