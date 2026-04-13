{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH MultiRowFormula_10_row_id_drop_0 AS (

  SELECT *
  
  FROM {{ ref('Resumo_de_CFOP_SAPXSATI__MultiRowFormula_10_row_id_drop_0')}}

),

Filter_11 AS (

  SELECT * 
  
  FROM MultiRowFormula_10_row_id_drop_0 AS in0
  
  WHERE (FLAG = '1')

),

AlteryxSelect_12 AS (

  SELECT * EXCLUDE ("21__03__2022", 
         "F2", 
         "F3", 
         "F4", 
         "F5", 
         "F6", 
         "F7", 
         "F8", 
         "F9", 
         "F10", 
         "F11", 
         "COLUNA RESUMO", 
         "F13", 
         "F14", 
         "LIVRO REGISTRO DE ENTRADAS - RE - MODELO P1SLASHA", 
         "F16", 
         "F18", 
         "F19", 
         "F20", 
         "F21", 
         "F22", 
         "F23", 
         "F27", 
         "F28", 
         "F30", 
         "F32", 
         "F33")
  
  FROM Filter_11 AS in0

),

Cleanse_13 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_12'], 
      [
        { "name": "F34", "dataType": "String" }, 
        { "name": "FLAG", "dataType": "Number" }, 
        { "name": "F31", "dataType": "String" }, 
        { "name": "F26", "dataType": "Float" }, 
        { "name": "F35", "dataType": "String" }, 
        { "name": "F24", "dataType": "String" }, 
        { "name": "F29", "dataType": "String" }, 
        { "name": "F25", "dataType": "String" }, 
        { "name": "F17", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['F17', 'F24', 'F25', 'F26', 'F29', 'F31', 'F34', 'F35', 'FLAG'], 
      true, 
      '', 
      true, 
      0, 
      true, 
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

MultiRowFormula_14_window AS (

  SELECT 
    *,
    LEAD(F17, 1) OVER (ORDER BY '1' ASC NULLS FIRST) AS F17_LEAD1
  
  FROM Cleanse_13 AS in0

),

SAIDASSAP02_22__26 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Resumo_de_CFOP_SAPXSATI', 'SAIDASSAP02_22__26') }}

),

AlteryxSelect_27 AS (

  SELECT 
    F12 AS "COLUNA RESUMO",
    * EXCLUDE ("F12")
  
  FROM SAIDASSAP02_22__26 AS in0

),

MultiRowFormula_28_row_id_0 AS (

  SELECT 
    (SEQ8()) AS PROPHECY_ROW_ID,
    *
  
  FROM AlteryxSelect_27 AS in0

),

MultiRowFormula_28 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_28_row_id_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM MultiRowFormula_28 AS in0

),

Filter_29 AS (

  SELECT * 
  
  FROM MultiRowFormula_28_row_id_drop_0 AS in0
  
  WHERE (FLAG = '1')

),

AlteryxSelect_30 AS (

  SELECT 
    CAST(F10 AS DOUBLE) AS "VALOR CONTABIL",
    CAST(F20 AS DOUBLE) AS "BASE DE CALCULO",
    CAST(F25 AS DOUBLE) AS "VALOR ICMS",
    CAST(F27 AS DOUBLE) AS ISENTAS,
    CAST(F28 AS DOUBLE) AS OUTRAS,
    * EXCLUDE ("30__03__2022", 
    "F2", 
    "F3", 
    "F4", 
    "F5", 
    "F6", 
    "F7", 
    "F8", 
    "F9", 
    "F11", 
    "LIVRO REGISTRO DE SAIDAS - RS - MODELO P2", 
    "F14", 
    "F18", 
    "F19", 
    "F21", 
    "F22", 
    "F23", 
    "F24", 
    "F26", 
    "F29", 
    "F10", 
    "F20", 
    "F25", 
    "F27", 
    "F28")
  
  FROM Filter_29 AS in0

),

Cleanse_31 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_30'], 
      [
        { "name": "VALOR CONTABIL", "dataType": "Float" }, 
        { "name": "BASE DE CALCULO", "dataType": "Float" }, 
        { "name": "VALOR ICMS", "dataType": "Float" }, 
        { "name": "ISENTAS", "dataType": "Float" }, 
        { "name": "OUTRAS", "dataType": "Float" }, 
        { "name": "F16", "dataType": "String" }, 
        { "name": "COLUNA RESUMO", "dataType": "String" }, 
        { "name": "FLAG", "dataType": "Number" }, 
        { "name": "F15", "dataType": "String" }, 
        { "name": "F17", "dataType": "Float" }
      ], 
      'keepOriginal', 
      [
        'VALOR CONTABIL', 
        'COLUNA RESUMO', 
        'F15', 
        'F16', 
        'F17', 
        'BASE DE CALCULO', 
        'VALOR ICMS', 
        'ISENTAS', 
        'OUTRAS', 
        'FLAG'
      ], 
      true, 
      '', 
      true, 
      0, 
      true, 
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

MultiRowFormula_32_window AS (

  SELECT 
    *,
    LEAD("COLUNA RESUMO", 1) OVER (ORDER BY '1' ASC NULLS FIRST) AS "COLUNA RESUMO_LEAD1"
  
  FROM Cleanse_31 AS in0

),

MultiRowFormula_32_row_id_0 AS (

  SELECT 
    (SEQ8()) AS PROPHECY_ROW_ID,
    *
  
  FROM MultiRowFormula_32_window AS in0

),

MultiRowFormula_32 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_32_row_id_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM MultiRowFormula_32 AS in0

),

Filter_33 AS (

  SELECT * 
  
  FROM MultiRowFormula_32_row_id_drop_0 AS in0
  
  WHERE (
          NOT(
            "NEW FIELD" = 2)
        )

),

AlteryxSelect_34 AS (

  SELECT 
    F15 AS CFOP,
    CAST(F17 AS STRING) AS F17,
    * EXCLUDE ("COLUNA RESUMO", "F17", "F15")
  
  FROM Filter_33 AS in0

),

MultiRowFormula_14_row_id_0 AS (

  SELECT 
    (SEQ8()) AS PROPHECY_ROW_ID,
    *
  
  FROM MultiRowFormula_14_window AS in0

),

MultiRowFormula_14 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_14_row_id_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM MultiRowFormula_14 AS in0

),

Filter_15 AS (

  SELECT * 
  
  FROM MultiRowFormula_14_row_id_drop_0 AS in0
  
  WHERE (
          NOT(
            "NEW FIELD" = 2)
        )

),

AlteryxSelect_16 AS (

  SELECT 
    CAST(F17 AS DOUBLE) AS "VALOR CONTABIL",
    F24 AS CFOP,
    CAST(F26 AS STRING) AS F26,
    CAST(F29 AS DOUBLE) AS "TIPO IMPOSTO",
    CAST(F31 AS DOUBLE) AS "BASE DE CALCULO",
    CAST(F34 AS DOUBLE) AS "VALOR ICMS",
    * EXCLUDE ("FLAG", "NEW FIELD", "F26", "F17", "F24", "F29", "F31", "F34")
  
  FROM Filter_15 AS in0

),

Formula_17_0 AS (

  SELECT 
    (
      CASE
        WHEN ("TIPO IMPOSTO" = 3)
          THEN "BASE DE CALCULO"
        ELSE NULL
      END
    ) AS OUTRAS,
    (
      CASE
        WHEN ("TIPO IMPOSTO" = 2)
          THEN "BASE DE CALCULO"
        ELSE NULL
      END
    ) AS ISENTAS,
    CAST((
      (
        REGEXP_REPLACE(
          (REGEXP_REPLACE((TO_CHAR(CAST(CFOP AS DOUBLE), 'FM999999999999999990')), ',', '__THS__')), 
          '__THS__', 
          '')
      )
      + (REGEXP_REPLACE((REGEXP_REPLACE((TO_CHAR(CAST(F26 AS DOUBLE), 'FM999999999999999990')), ',', '__THS__')), '__THS__', ''))
    ) AS STRING) AS CFOP,
    * EXCLUDE ("CFOP")
  
  FROM AlteryxSelect_16 AS in0

),

AlteryxSelect_18 AS (

  SELECT * EXCLUDE ("F25", "F26")
  
  FROM Formula_17_0 AS in0

),

Cleanse_19 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_18'], 
      [
        { "name": "OUTRAS", "dataType": "Float" }, 
        { "name": "ISENTAS", "dataType": "Float" }, 
        { "name": "CFOP", "dataType": "String" }, 
        { "name": "VALOR CONTABIL", "dataType": "Float" }, 
        { "name": "TIPO IMPOSTO", "dataType": "Float" }, 
        { "name": "BASE DE CALCULO", "dataType": "Float" }, 
        { "name": "VALOR ICMS", "dataType": "Float" }, 
        { "name": "F35", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['VALOR CONTABIL', 'CFOP', 'TIPO IMPOSTO', 'BASE DE CALCULO', 'VALOR ICMS', 'OUTRAS', 'ISENTAS'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      true, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

MultiRowFormula_20_row_id_0 AS (

  SELECT 
    (SEQ8()) AS PROPHECY_ROW_ID,
    *
  
  FROM Cleanse_19 AS in0

),

MultiRowFormula_20_0 AS (

  SELECT 
    (LEAD(OUTRAS, 1) OVER (PARTITION BY 1 ORDER BY PROPHECY_ROW_ID NULLS FIRST)) AS OUTRAS_LEAD1,
    *
  
  FROM MultiRowFormula_20_row_id_0 AS in0

),

MultiRowFormula_20_1 AS (

  SELECT 
    OUTRAS_LEAD1 AS OUTRAS,
    * EXCLUDE ("OUTRAS_LEAD1", "OUTRAS")
  
  FROM MultiRowFormula_20_0 AS in0

),

RESUMOCFOPSATI0_41 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Resumo_de_CFOP_SAPXSATI', 'RESUMOCFOPSATI0_41') }}

),

AlteryxSelect_42 AS (

  SELECT 
    "----------------------------------------------------------------------------------------------------------------------------------------------------------------" AS FIELD,
    * EXCLUDE ("----------------------------------------------------------------------------------------------------------------------------------------------------------------")
  
  FROM RESUMOCFOPSATI0_41 AS in0

),

MultiRowFormula_43_row_id_0 AS (

  SELECT 
    (SEQ8()) AS PROPHECY_ROW_ID,
    *
  
  FROM AlteryxSelect_42 AS in0

),

MultiRowFormula_43 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_43_row_id_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM MultiRowFormula_43 AS in0

),

Filter_44 AS (

  SELECT * 
  
  FROM MultiRowFormula_43_row_id_drop_0 AS in0
  
  WHERE (FLAG = '1')

),

Cleanse_46 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Filter_44'], 
      [
        { "name": "F16", "dataType": "Float" }, 
        { "name": "F49", "dataType": "Float" }, 
        { "name": "F42", "dataType": "Float" }, 
        { "name": "F12", "dataType": "Float" }, 
        { "name": "F38", "dataType": "Float" }, 
        { "name": "F27", "dataType": "Float" }, 
        { "name": "F9", "dataType": "Float" }, 
        { "name": "FIELD", "dataType": "String" }, 
        { "name": "F4", "dataType": "Float" }, 
        { "name": "F30", "dataType": "Float" }, 
        { "name": "F41", "dataType": "Float" }, 
        { "name": "F20", "dataType": "Float" }, 
        { "name": "F34", "dataType": "Float" }, 
        { "name": "F23", "dataType": "Float" }, 
        { "name": "F50", "dataType": "Float" }, 
        { "name": "F10", "dataType": "Float" }, 
        { "name": "F45", "dataType": "Float" }, 
        { "name": "FLAG", "dataType": "Number" }, 
        { "name": "F13", "dataType": "Float" }, 
        { "name": "F37", "dataType": "Float" }, 
        { "name": "F48", "dataType": "Float" }, 
        { "name": "F43", "dataType": "Float" }, 
        { "name": "F19", "dataType": "Float" }, 
        { "name": "F3", "dataType": "Float" }, 
        { "name": "F31", "dataType": "Float" }, 
        { "name": "F32", "dataType": "Float" }, 
        { "name": "F26", "dataType": "Float" }, 
        { "name": "F35", "dataType": "Float" }, 
        { "name": "F7", "dataType": "Float" }, 
        { "name": "F46", "dataType": "Float" }, 
        { "name": "F40", "dataType": "Float" }, 
        { "name": "F2", "dataType": "Float" }, 
        { "name": "F24", "dataType": "Float" }, 
        { "name": "F22", "dataType": "Float" }, 
        { "name": "F51", "dataType": "Float" }, 
        { "name": "F11", "dataType": "Float" }, 
        { "name": "F15", "dataType": "Float" }, 
        { "name": "F29", "dataType": "Float" }, 
        { "name": "F33", "dataType": "Float" }, 
        { "name": "F25", "dataType": "Float" }, 
        { "name": "F44", "dataType": "Float" }, 
        { "name": "F18", "dataType": "Float" }, 
        { "name": "F6", "dataType": "Float" }, 
        { "name": "F36", "dataType": "Float" }, 
        { "name": "F47", "dataType": "Float" }, 
        { "name": "F21", "dataType": "Float" }, 
        { "name": "F17", "dataType": "Float" }, 
        { "name": "F8", "dataType": "Float" }, 
        { "name": "F28", "dataType": "Float" }, 
        { "name": "F39", "dataType": "Float" }, 
        { "name": "F52", "dataType": "Float" }, 
        { "name": "F14", "dataType": "Float" }, 
        { "name": "F5", "dataType": "Float" }
      ], 
      'keepOriginal', 
      [
        'FIELD', 
        'F3', 
        'F4', 
        'F5', 
        'F6', 
        'F7', 
        'F8', 
        'F9', 
        'F10', 
        'F11', 
        'F12', 
        'F13', 
        'F14', 
        'F15', 
        'F16', 
        'F17', 
        'F18', 
        'F19', 
        'F20', 
        'F21', 
        'F22', 
        'F23', 
        'F24', 
        'F25', 
        'F26', 
        'F27', 
        'F28', 
        'F29', 
        'F30', 
        'F31', 
        'F32', 
        'F33', 
        'F34', 
        'F35', 
        'F36', 
        'F37', 
        'F38', 
        'F39', 
        'F40', 
        'F41', 
        'F42', 
        'F43', 
        'F44', 
        'F45', 
        'F46', 
        'F47', 
        'F48', 
        'F49', 
        'F50', 
        'F51', 
        'F52', 
        'FLAG', 
        'F2'
      ], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
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

TextToColumns_47 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Cleanse_46'], 
      'FIELD', 
      " ", 
      'splitColumns', 
      7, 
      'leaveExtraCharLastCol', 
      'FIELD', 
      'FIELD', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_47_dropGem_0 AS (

  SELECT 
    FIELD_1_FIELD AS "1",
    FIELD_2_FIELD AS "2",
    FIELD_3_FIELD AS "3",
    FIELD_4_FIELD AS "4",
    FIELD_5_FIELD AS "5",
    FIELD_6_FIELD AS "6",
    FIELD_7_FIELD AS "7",
    *
  
  FROM TextToColumns_47 AS in0

),

MultiRowFormula_48_window AS (

  SELECT 
    *,
    LEAD(1, 1) OVER (ORDER BY '1' ASC NULLS FIRST) AS "1_LEAD1"
  
  FROM TextToColumns_47_dropGem_0 AS in0

),

MultiRowFormula_48_row_id_0 AS (

  SELECT 
    (SEQ8()) AS PROPHECY_ROW_ID,
    *
  
  FROM MultiRowFormula_48_window AS in0

),

MultiRowFormula_48 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_48_row_id_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM MultiRowFormula_48 AS in0

),

Filter_49 AS (

  SELECT * 
  
  FROM MultiRowFormula_48_row_id_drop_0 AS in0
  
  WHERE (
          NOT(
            "NEW FIELD" = 2)
        )

),

AlteryxSelect_50 AS (

  SELECT * EXCLUDE ("FIELD", 
         "F2", 
         "F3", 
         "F4", 
         "F5", 
         "F6", 
         "F7", 
         "F8", 
         "F9", 
         "F10", 
         "F11", 
         "F12", 
         "F13", 
         "F14", 
         "F15", 
         "F16", 
         "F17", 
         "F18", 
         "F19", 
         "F20", 
         "F21", 
         "F22", 
         "F23", 
         "F24", 
         "F25", 
         "F26", 
         "F27", 
         "F28", 
         "F29", 
         "F30", 
         "F31", 
         "F32", 
         "F33", 
         "F34", 
         "F35", 
         "F36", 
         "F37", 
         "F38", 
         "F39", 
         "F40", 
         "F41", 
         "F42", 
         "F43", 
         "F44", 
         "F45", 
         "F46", 
         "F47", 
         "F48", 
         "F49", 
         "F50", 
         "F51", 
         "F52", 
         "FLAG", 
         "7", 
         "NEW FIELD")
  
  FROM Filter_49 AS in0

),

Filter_51 AS (

  SELECT * 
  
  FROM AlteryxSelect_50 AS in0
  
  WHERE (
          (
            (
              (NOT(6 IS NULL)) AND (
                    (
                      NOT(
                        1 = 'DO')
                    ) OR (1 IS NULL)
                  )
            )
            AND (
                  (
                    NOT(
                      1 = 'DE')
                  ) OR (1 IS NULL)
                )
          )
          AND (
                (
                  NOT(
                    1 = 'TOTAIS')
                ) OR (1 IS NULL)
              )
        )

),

MultiRowFormula_20_row_id_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM MultiRowFormula_20_1 AS in0

),

MultiRowFormula_21_row_id_0 AS (

  SELECT 
    (SEQ8()) AS PROPHECY_ROW_ID,
    *
  
  FROM MultiRowFormula_20_row_id_drop_0 AS in0

),

MultiRowFormula_21_0 AS (

  SELECT 
    (LEAD(OUTRAS, 1) OVER (PARTITION BY 1 ORDER BY PROPHECY_ROW_ID NULLS FIRST)) AS OUTRAS_LEAD1,
    *
  
  FROM MultiRowFormula_21_row_id_0 AS in0

),

MultiRowFormula_21_1 AS (

  SELECT 
    OUTRAS_LEAD1 AS OUTRAS,
    * EXCLUDE ("OUTRAS_LEAD1", "OUTRAS")
  
  FROM MultiRowFormula_21_0 AS in0

),

MultiRowFormula_21_row_id_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM MultiRowFormula_21_1 AS in0

),

MultiRowFormula_22_row_id_0 AS (

  SELECT 
    (SEQ8()) AS PROPHECY_ROW_ID,
    *
  
  FROM MultiRowFormula_21_row_id_drop_0 AS in0

),

MultiRowFormula_22_0 AS (

  SELECT 
    (LEAD(ISENTAS, 1) OVER (PARTITION BY 1 ORDER BY PROPHECY_ROW_ID NULLS FIRST)) AS ISENTAS_LEAD1,
    *
  
  FROM MultiRowFormula_22_row_id_0 AS in0

),

MultiRowFormula_22_1 AS (

  SELECT 
    ISENTAS_LEAD1 AS ISENTAS,
    * EXCLUDE ("ISENTAS_LEAD1", "ISENTAS")
  
  FROM MultiRowFormula_22_0 AS in0

),

MultiRowFormula_22_row_id_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM MultiRowFormula_22_1 AS in0

),

Filter_23 AS (

  SELECT * 
  
  FROM MultiRowFormula_22_row_id_drop_0 AS in0
  
  WHERE (
          NOT(
            CFOP = 0)
        )

),

Formula_35_0 AS (

  SELECT 
    CAST((
      (
        REGEXP_REPLACE(
          (REGEXP_REPLACE((TO_CHAR(CAST(CFOP AS DOUBLE), 'FM999999999999999990')), ',', '__THS__')), 
          '__THS__', 
          '')
      )
      + (REGEXP_REPLACE((REGEXP_REPLACE((TO_CHAR(CAST(F17 AS DOUBLE), 'FM999999999999999990')), ',', '__THS__')), '__THS__', ''))
    ) AS STRING) AS CFOP,
    * EXCLUDE ("CFOP")
  
  FROM AlteryxSelect_34 AS in0

),

Formula_53_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(1, '.', '')) AS STRING) AS "1",
    CAST((REGEXP_REPLACE(2, '.', '')) AS STRING) AS "2",
    CAST((REGEXP_REPLACE(3, '.', '')) AS STRING) AS "3",
    CAST((REGEXP_REPLACE(4, '.', '')) AS STRING) AS "4",
    CAST((REGEXP_REPLACE(5, '.', '')) AS STRING) AS "5",
    CAST((REGEXP_REPLACE(6, '.', '')) AS STRING) AS "6",
    * EXCLUDE ("4", "5", "6", "1", "2", "3")
  
  FROM Filter_51 AS in0

),

Formula_53_1 AS (

  SELECT 
    CAST((REGEXP_REPLACE(2, ',', '.')) AS STRING) AS "2",
    CAST((REGEXP_REPLACE(3, ',', '.')) AS STRING) AS "3",
    CAST((REGEXP_REPLACE(4, ',', '.')) AS STRING) AS "4",
    CAST((REGEXP_REPLACE(5, ',', '.')) AS STRING) AS "5",
    CAST((REGEXP_REPLACE(6, ',', '.')) AS STRING) AS "6",
    * EXCLUDE ("4", "5", "6", "2", "3")
  
  FROM Formula_53_0 AS in0

),

AlteryxSelect_54 AS (

  SELECT 
    1 AS CFOP,
    CAST(2 AS DOUBLE) AS "VALOR CONTABIL",
    CAST(3 AS DOUBLE) AS "BASE DE CALCULO",
    CAST(4 AS DOUBLE) AS "VALOR ICMS",
    CAST(5 AS DOUBLE) AS ISENTAS,
    CAST(6 AS DOUBLE) AS OUTRAS,
    * EXCLUDE ("1", "2", "3", "4", "5", "6")
  
  FROM Formula_53_1 AS in0

),

AlteryxSelect_24 AS (

  SELECT 
    CFOP AS CFOP,
    "VALOR CONTABIL" AS "VALOR CONTABIL",
    "BASE DE CALCULO" AS "BASE DE CALCULO",
    "VALOR ICMS" AS "VALOR ICMS",
    CAST(ISENTAS AS DOUBLE) AS ISENTAS,
    CAST(OUTRAS AS DOUBLE) AS OUTRAS
  
  FROM Filter_23 AS in0

),

AlteryxSelect_36 AS (

  SELECT 
    CFOP AS CFOP,
    "VALOR CONTABIL" AS "VALOR CONTABIL",
    "BASE DE CALCULO" AS "BASE DE CALCULO",
    "VALOR ICMS" AS "VALOR ICMS",
    ISENTAS AS ISENTAS,
    OUTRAS AS OUTRAS
  
  FROM Formula_35_0 AS in0

),

Filter_38 AS (

  SELECT * 
  
  FROM AlteryxSelect_36 AS in0
  
  WHERE (
          NOT(
            CFOP = 0)
        )

),

Union_39 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_24', 'Filter_38'], 
      [
        '[{"name": "CFOP", "dataType": "String"}, {"name": "VALOR CONTABIL", "dataType": "Float"}, {"name": "BASE DE CALCULO", "dataType": "Float"}, {"name": "VALOR ICMS", "dataType": "Float"}, {"name": "ISENTAS", "dataType": "Float"}, {"name": "OUTRAS", "dataType": "Float"}]', 
        '[{"name": "CFOP", "dataType": "String"}, {"name": "VALOR CONTABIL", "dataType": "Float"}, {"name": "BASE DE CALCULO", "dataType": "Float"}, {"name": "VALOR ICMS", "dataType": "Float"}, {"name": "ISENTAS", "dataType": "Float"}, {"name": "OUTRAS", "dataType": "Float"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_55_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in0."VALOR ICMS"
        ELSE NULL
      END
    ) AS "VALOR ICMS SAP",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in0."VALOR CONTABIL"
        ELSE NULL
      END
    ) AS "VALOR CONTABIL SAP",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in1.CFOP
        ELSE NULL
      END
    ) AS "CFOP SATI",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in0.OUTRAS
        ELSE NULL
      END
    ) AS "OUTRAS SAP",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in1."VALOR ICMS"
        ELSE NULL
      END
    ) AS "VALOR ICMS SATI",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in1.OUTRAS
        ELSE NULL
      END
    ) AS "OUTRAS SATI",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in1."VALOR CONTABIL"
        ELSE NULL
      END
    ) AS "VALOR CONTABIL SATI",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in0.CFOP
        ELSE NULL
      END
    ) AS "CFOP SAP",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN NULL
        ELSE in0."VALOR ICMS"
      END
    ) AS "VALOR ICMS",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in1."BASE DE CALCULO"
        ELSE NULL
      END
    ) AS "BASE DE CALCULO SATI",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN NULL
        ELSE in0."BASE DE CALCULO"
      END
    ) AS "BASE DE CALCULO",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in0."BASE DE CALCULO"
        ELSE NULL
      END
    ) AS "BASE DE CALCULO SAP",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN NULL
        ELSE in0.ISENTAS
      END
    ) AS ISENTAS,
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN NULL
        ELSE in0.OUTRAS
      END
    ) AS OUTRAS,
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN NULL
        ELSE in0.CFOP
      END
    ) AS CFOP,
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in0.ISENTAS
        ELSE NULL
      END
    ) AS "ISENTAS SAP",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN NULL
        ELSE in0."VALOR CONTABIL"
      END
    ) AS "VALOR CONTABIL",
    (
      CASE
        WHEN (in0.CFOP = in1.CFOP)
          THEN in1.ISENTAS
        ELSE NULL
      END
    ) AS "ISENTAS SATI",
    in0.* EXCLUDE ("CFOP", "VALOR CONTABIL", "BASE DE CALCULO", "VALOR ICMS", "ISENTAS", "OUTRAS"),
    in1.* EXCLUDE ("CFOP", "VALOR CONTABIL", "BASE DE CALCULO", "VALOR ICMS", "ISENTAS", "OUTRAS")
  
  FROM Union_39 AS in0
  FULL JOIN AlteryxSelect_54 AS in1
     ON (in0.CFOP = in1.CFOP)

),

Cleanse_58 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_55_left_UnionFullOuter'], 
      [
        { "name": "VALOR ICMS SAP", "dataType": "Float" }, 
        { "name": "VALOR CONTABIL SAP", "dataType": "Float" }, 
        { "name": "CFOP SATI", "dataType": "Number" }, 
        { "name": "OUTRAS SAP", "dataType": "Float" }, 
        { "name": "VALOR ICMS SATI", "dataType": "Float" }, 
        { "name": "OUTRAS SATI", "dataType": "Float" }, 
        { "name": "VALOR CONTABIL SATI", "dataType": "Float" }, 
        { "name": "CFOP SAP", "dataType": "String" }, 
        { "name": "VALOR ICMS", "dataType": "Float" }, 
        { "name": "BASE DE CALCULO SATI", "dataType": "Float" }, 
        { "name": "BASE DE CALCULO", "dataType": "Float" }, 
        { "name": "BASE DE CALCULO SAP", "dataType": "Float" }, 
        { "name": "ISENTAS", "dataType": "Float" }, 
        { "name": "OUTRAS", "dataType": "Float" }, 
        { "name": "CFOP", "dataType": "String" }, 
        { "name": "ISENTAS SAP", "dataType": "Float" }, 
        { "name": "VALOR CONTABIL", "dataType": "Float" }, 
        { "name": "ISENTAS SATI", "dataType": "Float" }, 
        { "name": "FIELD_2_FIELD", "dataType": "String" }, 
        { "name": "FIELD_1_FIELD", "dataType": "String" }, 
        { "name": "FIELD_5_FIELD", "dataType": "String" }, 
        { "name": "FIELD_4_FIELD", "dataType": "String" }, 
        { "name": "FIELD_7_FIELD", "dataType": "String" }, 
        { "name": "FIELD_6_FIELD", "dataType": "String" }, 
        { "name": "FIELD_3_FIELD", "dataType": "String" }
      ], 
      'keepOriginal', 
      [
        'CFOP SAP', 
        'VALOR CONTABIL SAP', 
        'BASE DE CALCULO SAP', 
        'VALOR ICMS SAP', 
        'ISENTAS SAP', 
        'OUTRAS SAP', 
        'CFOP SATI', 
        'VALOR CONTABIL SATI', 
        'BASE DE CALCULO SATI', 
        'VALOR ICMS SATI', 
        'ISENTAS SATI', 
        'OUTRAS SATI'
      ], 
      true, 
      '', 
      true, 
      0, 
      true, 
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

Formula_57_0 AS (

  SELECT 
    CAST((CAST("VALOR CONTABIL SAP" AS DECIMAL (19, 9)) - CAST("VALOR CONTABIL SATI" AS DECIMAL (19, 9))) AS DOUBLE) AS "DIFERENCA VALOR CONTABIL",
    CAST((CAST("BASE DE CALCULO SAP" AS DECIMAL (19, 9)) - CAST("BASE DE CALCULO SATI" AS DECIMAL (19, 9))) AS DOUBLE) AS "DIFERENCA BASE DE CALCULO",
    CAST((CAST("VALOR ICMS SAP" AS DECIMAL (19, 9)) - CAST("VALOR ICMS SATI" AS DECIMAL (19, 9))) AS DOUBLE) AS "DIFERENCA VALOR ICMS",
    CAST((CAST("ISENTAS SAP" AS DECIMAL (19, 9)) - CAST("ISENTAS SATI" AS DECIMAL (19, 9))) AS DOUBLE) AS "DIFERENCA ISENTAS",
    CAST((CAST("OUTRAS SAP" AS DECIMAL (19, 9)) - CAST("OUTRAS SATI" AS DECIMAL (19, 9))) AS DOUBLE) AS "DIFERENCA OUTRAS",
    *
  
  FROM Cleanse_58 AS in0

)

SELECT *

FROM Formula_57_0
