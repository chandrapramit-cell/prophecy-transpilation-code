{{
  config({    
    "materialized": "table",
    "alias": "DbFileOutput_18_182",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Query__Sheet1___144 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DQ_Macro', 'Query__Sheet1___144') }}

),

AlteryxSelect_196 AS (

  SELECT * 
  
  FROM Query__Sheet1___144 AS in0

),

AlteryxSelect_25 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__AlteryxSelect_25')}}

),

CountRecords_177 AS (

  SELECT COUNT(*) AS `Count`
  
  FROM AlteryxSelect_25 AS in0

),

JoinMultiple_179_in0 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_0`
  
  FROM CountRecords_177

),

AlteryxSelect_20 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__AlteryxSelect_20')}}

),

CountRecords_178 AS (

  SELECT COUNT(*) AS `Count`
  
  FROM AlteryxSelect_20 AS in0

),

JoinMultiple_179_in2 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_2`
  
  FROM CountRecords_178

),

AlteryxSelect_19 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__AlteryxSelect_19')}}

),

CountRecords_176 AS (

  SELECT COUNT(*) AS `Count`
  
  FROM AlteryxSelect_19 AS in0

),

JoinMultiple_179_in1 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_1`
  
  FROM CountRecords_176

),

AlteryxSelect_183 AS (

  SELECT FileName AS FileName
  
  FROM Query__Sheet1___144 AS in0

),

JoinMultiple_179_in4 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_4`
  
  FROM AlteryxSelect_183

),

Join_72_inner AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__Join_72_inner')}}

),

Summarize_200 AS (

  {{ prophecy_basics.ToDo('Error encountered while transpiling tool: Summarize, exception: None.get') }}

),

JoinMultiple_179_in5 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_5`
  
  FROM Summarize_200

),

Formula_163_0 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__Formula_163_0')}}

),

JoinMultiple_179_in3 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_3`
  
  FROM Formula_163_0

),

JoinMultiple_179 AS (

  SELECT 
    in0.COUNT AS `Count`,
    in3.`Same File Types` AS `Same File Types`,
    in3.`Expected File Format` AS `Expected File Format`,
    in4.FileName AS FileName,
    in3.`Actual File Format` AS `Actual File Format`,
    in3.`Actual Data Type` AS `Actual Data Type`,
    in3.`Expected Data Type` AS `Expected Data Type`,
    in1.COUNT AS Input_hash2_Count,
    in2.COUNT AS Input_hash3_Count
  
  FROM JoinMultiple_179_in0 AS in0
  FULL JOIN JoinMultiple_179_in1 AS in1
     ON (in0.recordPositionForJoin_0 = in1.recordPositionForJoin_1)
  FULL JOIN JoinMultiple_179_in2 AS in2
     ON (coalesce(in0.recordPositionForJoin_0, in1.recordPositionForJoin_1) = in2.recordPositionForJoin_2)
  FULL JOIN JoinMultiple_179_in3 AS in3
     ON (coalesce(in0.recordPositionForJoin_0, in1.recordPositionForJoin_1, in2.recordPositionForJoin_2) = in3.recordPositionForJoin_3)
  FULL JOIN JoinMultiple_179_in4 AS in4
     ON (
      coalesce(
        in0.recordPositionForJoin_0, 
        in1.recordPositionForJoin_1, 
        in2.recordPositionForJoin_2, 
        in3.recordPositionForJoin_3) = in4.recordPositionForJoin_4
    )
  FULL JOIN JoinMultiple_179_in5 AS in5
     ON (
      coalesce(
        in0.recordPositionForJoin_0, 
        in1.recordPositionForJoin_1, 
        in2.recordPositionForJoin_2, 
        in3.recordPositionForJoin_3, 
        in4.recordPositionForJoin_4) = in5.recordPositionForJoin_5
    )

),

Formula_180 AS (

  SELECT *
  
  FROM JoinMultiple_179

),

AlteryxSelect_191 AS (

  SELECT * 
  
  FROM Formula_180 AS in0

),

AppendFields_193 AS (

  SELECT * 
  
  FROM AlteryxSelect_191 AS in0
  INNER JOIN AlteryxSelect_196 AS in1
     ON true

),

AlteryxSelect_195 AS (

  SELECT * 
  
  FROM AppendFields_193 AS in0

)

SELECT *

FROM AlteryxSelect_195
