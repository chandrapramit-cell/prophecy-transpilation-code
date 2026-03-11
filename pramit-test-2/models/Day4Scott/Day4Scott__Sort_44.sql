{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH GenerateRows_34 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Day4Scott', 'GenerateRows_34') }}

),

AlteryxSelect_33 AS (

  SELECT *
  
  FROM {{ ref('Day4Scott__AlteryxSelect_33')}}

),

AlteryxSelect_35 AS (

  SELECT 
    RowCount AS Direction,
    * EXCEPT (`RowCount`)
  
  FROM GenerateRows_34 AS in0

),

Formula_37_0 AS (

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
  
  FROM AlteryxSelect_35 AS in0

),

Filter_38 AS (

  SELECT * 
  
  FROM Formula_37_0 AS in0
  
  WHERE (
          (((Row2 > 0) AND ((Row3 > 0) AND (Row4 > 0))) AND ((Col2 > 0) AND ((Col3 > 0) AND (Col4 > 0))))
          AND (
                ((Row2 < 141) AND ((Row3 < 141) AND (Row4 < 141)))
                AND ((Col2 < 141) AND ((Col3 < 141) AND (Col4 < 141)))
              )
        )

),

Join_39_inner AS (

  SELECT 
    in1.VALUE AS Value2,
    in0.*
  
  FROM Filter_38 AS in0
  INNER JOIN AlteryxSelect_33 AS in1
     ON ((in0.Row2 = in1.variableRow) AND (in0.Col2 = in1.Column))

),

Join_40_inner AS (

  SELECT 
    in1.VALUE AS Value3,
    in0.*
  
  FROM Join_39_inner AS in0
  INNER JOIN AlteryxSelect_33 AS in1
     ON ((in0.Row3 = in1.variableRow) AND (in0.Col3 = in1.Column))

),

Join_41_inner AS (

  SELECT 
    in1.VALUE AS Value4,
    in0.*
  
  FROM Join_40_inner AS in0
  INNER JOIN AlteryxSelect_33 AS in1
     ON ((in0.Row4 = in1.variableRow) AND (in0.Col4 = in1.Column))

),

Filter_42 AS (

  SELECT * 
  
  FROM Join_41_inner AS in0
  
  WHERE (((Value2 = 'M') AND (Value3 = 'A')) AND (Value4 = 'S'))

),

Sort_44 AS (

  SELECT * 
  
  FROM Filter_42 AS in0
  
  ORDER BY variableRow ASC, Column ASC

)

SELECT *

FROM Sort_44
