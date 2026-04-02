{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_12_AlteryxSelect_1210",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH aka_Server_UYDB_1043 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('productos_12_part1', 'aka_Server_UYDB_1043') }}

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

)

SELECT *

FROM AlteryxSelect_1210
