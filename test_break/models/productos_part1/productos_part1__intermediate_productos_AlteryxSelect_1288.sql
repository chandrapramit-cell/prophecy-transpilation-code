{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_AlteryxSelect_1288",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH aka_Server_UYDB_1036 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('productos_part1', 'aka_Server_UYDB_1036') }}

),

Filter_1286 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_1036 AS in0
  
  WHERE (banco_cobrador = CAST('137' AS INTEGER))

),

Summarize_1255 AS (

  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM Filter_1286 AS in0

),

Sample_1257 AS (

  {{ prophecy_basics.Sample('Summarize_1255', [], 1002, 'firstN', 17) }}

),

RecordID_1258 AS (

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

Summarize_1259 AS (

  SELECT DISTINCT idf_pers_ods_pagador AS idf_pers_ods
  
  FROM Join_1253_inner AS in0

),

AppendFields_1260 AS (

  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_1258 AS in0
  INNER JOIN Summarize_1259 AS in1
     ON TRUE

),

Summarize_1251 AS (

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

  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_1250'], 
      ['12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'], 
      'editPrefixSuffix', 
      [
        'idf_pers_ods', 
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

Filter_1286_reject AS (

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

  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM Filter_1286_reject AS in0

),

Sample_1276 AS (

  {{ prophecy_basics.Sample('Summarize_1274', [], 1002, 'firstN', 17) }}

),

RecordID_1277 AS (

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

  SELECT DISTINCT idf_pers_ods_pagador AS idf_pers_ods
  
  FROM Join_1272_inner AS in0

),

AppendFields_1283 AS (

  SELECT 
    in1.idf_pers_ods AS idf_pers_ods,
    in0.MesId AS MesId,
    in0.AAAAMM AS AAAAMM
  
  FROM RecordID_1277 AS in0
  INNER JOIN Summarize_1282 AS in1
     ON TRUE

),

Summarize_1281 AS (

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

CrossTab_1270 AS (

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

  {{
    prophecy_basics.MultiColumnRename(
      ['CrossTab_1270'], 
      ['12', '8', '4', '15', '11', '9', '13', '16', '5', '10', '6', '17', '14', '2', '18', '7', '3'], 
      'editPrefixSuffix', 
      [
        'idf_pers_ods', 
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

Formula_1247_0 AS (

  SELECT 
    CAST(18 AS INTEGER) AS CodProducto,
    CAST('# empleados en Santander' AS string) AS Col1,
    *
  
  FROM MultiFieldFormula_1249 AS in0

),

MultiFieldFormula_1269 AS (

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

  SELECT 
    CAST(18 AS INTEGER) AS CodProducto,
    CAST('# empleados en otros Bancos' AS string) AS Col1,
    *
  
  FROM MultiFieldFormula_1269 AS in0

),

Union_1287 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_1247_0', 'Formula_1267_0'], 
      [
        '[{"name": "CodProducto", "dataType": "Integer"}, {"name": "Col1", "dataType": "String"}, {"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]', 
        '[{"name": "CodProducto", "dataType": "Integer"}, {"name": "Col1", "dataType": "String"}, {"name": "Col16", "dataType": "String"}, {"name": "Col4", "dataType": "String"}, {"name": "Col12", "dataType": "String"}, {"name": "Col9", "dataType": "String"}, {"name": "Col8", "dataType": "String"}, {"name": "Col3", "dataType": "String"}, {"name": "Col17", "dataType": "String"}, {"name": "Col10", "dataType": "String"}, {"name": "Col5", "dataType": "String"}, {"name": "Col18", "dataType": "String"}, {"name": "Col6", "dataType": "String"}, {"name": "Col13", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "Col11", "dataType": "String"}, {"name": "Col14", "dataType": "String"}, {"name": "Col15", "dataType": "String"}, {"name": "Col7", "dataType": "String"}, {"name": "Col2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_1288 AS (

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

)

SELECT *

FROM AlteryxSelect_1288
