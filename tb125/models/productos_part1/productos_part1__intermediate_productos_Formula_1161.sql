{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_Formula_1161",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH aka_Server_UYDB_1036 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('productos_part1', 'aka_Server_UYDB_1036') }}

),

Summarize_1166 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT DISTINCT AAAAMM AS AAAAMM
  
  FROM aka_Server_UYDB_1036 AS in0

),

Sample_1168 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  {{
    prophecy_basics.Sample(
      ['Summarize_1166'], 
      '[{"name": "AAAAMM", "dataType": "String"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      17
    )
  }}

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

Formula_1161_0 AS (

  {#VisualGroup: CobrodeSueldosProd9#}
  SELECT 
    CAST(9 AS INTEGER) AS CodProducto,
    CAST('Importe Cobrado' AS string) AS Col1,
    *
  
  FROM DynamicRename_1160 AS in0

)

{#VisualGroup: Intermediate Target Nodes#}
SELECT *

FROM Formula_1161_0
