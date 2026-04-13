{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_15 AS (

  SELECT * 
  
  FROM {{ ref('seed_15')}}

),

TextInput_15_cast AS (

  SELECT CAST(FIELD1 AS STRING) AS FIELD1
  
  FROM TextInput_15 AS in0

),

RecordID_9 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_15_cast'], 
      'incremental_id', 
      'RECORDID', 
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

Cleanse_5 AS (

  {{
    prophecy_basics.DataCleansing(
      ['RecordID_9'], 
      [{ "name": "RECORDID", "dataType": "Number" }, { "name": "FIELD1", "dataType": "String" }], 
      'keepOriginal', 
      ['FIELD1'], 
      true, 
      '', 
      true, 
      0, 
      false, 
      false, 
      true, 
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

TextToColumns_2 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Cleanse_5'], 
      'FIELD1', 
      "@", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'FIELD1', 
      'FIELD1', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_2_dropGem_0 AS (

  SELECT 
    FIELD1_1_FIELD1 AS "1",
    FIELD1_2_FIELD1 AS "2",
    *
  
  FROM TextToColumns_2 AS in0

),

TextToColumns_3 AS (

  {{
    prophecy_basics.TextToColumns(
      ['TextToColumns_2_dropGem_0'], 
      '1', 
      ", ", 
      'splitColumns', 
      3, 
      'leaveExtraCharLastCol', 
      '1', 
      '1', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_3_dropGem_0 AS (

  SELECT 
    "1_1_1" AS "11",
    "1_2_1" AS "12",
    "1_3_1" AS "13",
    *
  
  FROM TextToColumns_3 AS in0

),

TextToColumns_4 AS (

  {{
    prophecy_basics.TextToColumns(
      ['TextToColumns_3_dropGem_0'], 
      '2', 
      ", ", 
      'splitColumns', 
      3, 
      'leaveExtraCharLastCol', 
      '2', 
      '2', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_4_dropGem_0 AS (

  SELECT 
    "2_1_2" AS "21",
    "2_2_2" AS "22",
    "2_3_2" AS "23",
    *
  
  FROM TextToColumns_4 AS in0

),

AlteryxSelect_6 AS (

  SELECT 
    CAST("11" AS INTEGER) AS X,
    CAST("12" AS INTEGER) AS Y,
    CAST("13" AS INTEGER) AS Z,
    CAST("21" AS INTEGER) AS VX,
    CAST("22" AS INTEGER) AS VY,
    CAST("23" AS INTEGER) AS VZ,
    * EXCLUDE ("FIELD1", "1", "2", "11", "12", "13", "21", "22", "23")
  
  FROM TextToColumns_4_dropGem_0 AS in0

),

Formula_7_0 AS (

  SELECT 
    CAST((VY / VX) AS DOUBLE) AS M,
    *
  
  FROM AlteryxSelect_6 AS in0

),

Formula_7_1 AS (

  SELECT 
    (Y - (M * X)) AS B,
    *
  
  FROM Formula_7_0 AS in0

),

AppendFields_20 AS (

  SELECT 
    in0.RECORDID AS SOURCE_RECORDID,
    in0.X AS SOURCE_X,
    in0.Y AS SOURCE_Y,
    in0.Z AS SOURCE_Z,
    in0.VX AS SOURCE_VX,
    in0.VY AS SOURCE_VY,
    in0.VZ AS SOURCE_VZ,
    in0.M AS SOURCE_M,
    in0.B AS SOURCE_B,
    in0.* EXCLUDE ("RECORDID", 
    "X", 
    "Y", 
    "Z", 
    "VX", 
    "VY", 
    "VZ", 
    "M", 
    "B", 
    "FIELD1_2_FIELD1", 
    "1_3_1", 
    "2_1_2", 
    "2_3_2", 
    "1_1_1", 
    "FIELD1_1_FIELD1", 
    "2_2_2", 
    "1_2_1"),
    in1.*
  
  FROM Formula_7_1 AS in0
  INNER JOIN Formula_7_1 AS in1
     ON TRUE

),

Filter_12_to_Filter_13 AS (

  SELECT * 
  
  FROM AppendFields_20 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  RECORDID = SOURCE_RECORDID)
              ) OR (RECORDID IS NULL)
            )
            OR (SOURCE_RECORDID IS NULL)
          )
          AND (
                (
                  (
                    NOT(
                      M = SOURCE_M)
                  ) OR (M IS NULL)
                ) OR (SOURCE_M IS NULL)
              )
        )

),

Formula_14_0 AS (

  SELECT 
    ((SOURCE_B - B) / (M - SOURCE_M)) AS "X-INT",
    *
  
  FROM Filter_12_to_Filter_13 AS in0

),

Formula_14_1 AS (

  SELECT 
    ((M * "X-INT") + B) AS "Y-INT",
    *
  
  FROM Formula_14_0 AS in0

),

Formula_14_2 AS (

  SELECT 
    (
      CASE
        WHEN (
          (
            (
              ((((((VX > 0) AND ("X-INT" < X)) OR (VX < 0)) AND ("X-INT" > X)) OR (VY > 0)) AND ("Y-INT" < Y))
              OR (VY < 0)
            )
            AND ("Y-INT" > Y)
          )
          OR (
               (
                 (
                   (
                     ((((SOURCE_VX > 0) AND ("X-INT" < SOURCE_X)) OR (SOURCE_VX < 0)) AND ("X-INT" > SOURCE_X))
                     OR (SOURCE_VY > 0)
                   )
                   AND ("Y-INT" < SOURCE_Y)
                 )
                 OR (SOURCE_VY < 0)
               )
               AND ("Y-INT" > SOURCE_Y)
             )
        )
          THEN 'Y'
        ELSE 'N'
      END
    ) AS PASTLINE,
    *
  
  FROM Formula_14_1 AS in0

),

Filter_17 AS (

  SELECT * 
  
  FROM Formula_14_2 AS in0
  
  WHERE (PASTLINE = 'n')

)

SELECT *

FROM Filter_17
