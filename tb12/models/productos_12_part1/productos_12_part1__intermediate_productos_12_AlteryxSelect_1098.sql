{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_12_AlteryxSelect_1098",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH aka_Server_UYDB_1041 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('productos_12_part1', 'aka_Server_UYDB_1041') }}

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

)

SELECT *

FROM AlteryxSelect_1098
