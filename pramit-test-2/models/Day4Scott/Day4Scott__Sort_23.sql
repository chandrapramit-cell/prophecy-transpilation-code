{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH GenerateRows_13 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Day4Scott', 'GenerateRows_13') }}

),

AlteryxSelect_14 AS (

  SELECT 
    RowCount AS Direction,
    * EXCEPT (`RowCount`)
  
  FROM GenerateRows_13 AS in0

),

Formula_16_0 AS (

  SELECT 
    (
      CASE
        WHEN (CAST(Direction AS FLOAT64) IN (1, 5, 6))
          THEN (variableRow - 1)
        WHEN (CAST(Direction AS FLOAT64) IN (3, 7, 8))
          THEN (variableRow + 1)
        ELSE variableRow
      END
    ) AS Row2,
    (
      CASE
        WHEN (CAST(Direction AS FLOAT64) IN (1, 5, 6))
          THEN (variableRow - 2)
        WHEN (CAST(Direction AS FLOAT64) IN (3, 7, 8))
          THEN (variableRow + 2)
        ELSE variableRow
      END
    ) AS Row3,
    (
      CASE
        WHEN (CAST(Direction AS FLOAT64) IN (1, 5, 6))
          THEN (variableRow - 3)
        WHEN (CAST(Direction AS FLOAT64) IN (3, 7, 8))
          THEN (variableRow + 3)
        ELSE variableRow
      END
    ) AS Row4,
    (
      CASE
        WHEN (CAST(Direction AS FLOAT64) IN (4, 5, 7))
          THEN (Column - 1)
        WHEN (CAST(Direction AS FLOAT64) IN (2, 6, 8))
          THEN (Column + 1)
        ELSE Column
      END
    ) AS Col2,
    (
      CASE
        WHEN (CAST(Direction AS FLOAT64) IN (4, 5, 7))
          THEN (Column - 2)
        WHEN (CAST(Direction AS FLOAT64) IN (2, 6, 8))
          THEN (Column + 2)
        ELSE Column
      END
    ) AS Col3,
    (
      CASE
        WHEN (CAST(Direction AS FLOAT64) IN (4, 5, 7))
          THEN (Column - 3)
        WHEN (CAST(Direction AS FLOAT64) IN (2, 6, 8))
          THEN (Column + 3)
        ELSE Column
      END
    ) AS Col4,
    *
  
  FROM AlteryxSelect_14 AS in0

),

Filter_17 AS (

  SELECT * 
  
  FROM Formula_16_0 AS in0
  
  WHERE (
          (((Row2 > 0) AND ((Row3 > 0) AND (Row4 > 0))) AND ((Col2 > 0) AND ((Col3 > 0) AND (Col4 > 0))))
          AND (((Row2 < 11) AND ((Row3 < 11) AND (Row4 < 11))) AND ((Col2 < 11) AND ((Col3 < 11) AND (Col4 < 11))))
        )

),

AlteryxSelect_11 AS (

  SELECT *
  
  FROM {{ ref('Day4Scott__AlteryxSelect_11')}}

),

Join_18_inner AS (

  SELECT 
    in1.VALUE AS Value2,
    in0.*
  
  FROM Filter_17 AS in0
  INNER JOIN AlteryxSelect_11 AS in1
     ON ((in0.Row2 = in1.variableRow) AND (in0.Col2 = in1.Column))

),

Join_19_inner AS (

  SELECT 
    in1.VALUE AS Value3,
    in0.*
  
  FROM Join_18_inner AS in0
  INNER JOIN AlteryxSelect_11 AS in1
     ON ((in0.Row3 = in1.variableRow) AND (in0.Col3 = in1.Column))

),

Join_20_inner AS (

  SELECT 
    in1.VALUE AS Value4,
    in0.*
  
  FROM Join_19_inner AS in0
  INNER JOIN AlteryxSelect_11 AS in1
     ON ((in0.Row4 = in1.variableRow) AND (in0.Col4 = in1.Column))

),

Filter_21 AS (

  SELECT * 
  
  FROM Join_20_inner AS in0
  
  WHERE (((Value2 = 'M') AND (Value3 = 'A')) AND (Value4 = 'S'))

),

Sort_23 AS (

  SELECT * 
  
  FROM Filter_21 AS in0
  
  ORDER BY variableRow ASC, Column ASC

)

SELECT *

FROM Sort_23
