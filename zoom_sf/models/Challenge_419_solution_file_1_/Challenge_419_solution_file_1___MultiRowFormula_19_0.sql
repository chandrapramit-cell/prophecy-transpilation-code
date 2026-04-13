{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_3 AS (

  SELECT * 
  
  FROM {{ ref('seed_3')}}

),

TextInput_3_cast AS (

  SELECT 
    CAST("STUFFED ANIMAL" AS STRING) AS "STUFFED ANIMAL",
    PRICE AS PRICE,
    "YEAR" AS YEAR
  
  FROM TextInput_3 AS in0

),

TextInput_2 AS (

  SELECT * 
  
  FROM {{ ref('seed_2')}}

),

TextInput_2_cast AS (

  SELECT 
    CAST(TREAT AS STRING) AS TREAT,
    PRICE AS PRICE,
    "YEAR" AS YEAR
  
  FROM TextInput_2 AS in0

),

Union_4 AS (

  {{
    prophecy_basics.UnionByName(
      ['TextInput_2_cast', 'TextInput_3_cast'], 
      [
        '[{"name": "TREAT", "dataType": "String"}, {"name": "PRICE", "dataType": "Number"}, {"name": "YEAR", "dataType": "Number"}]', 
        '[{"name": "STUFFED ANIMAL", "dataType": "String"}, {"name": "PRICE", "dataType": "Number"}, {"name": "YEAR", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

TextInput_6 AS (

  SELECT * 
  
  FROM {{ ref('seed_6')}}

),

TextInput_6_cast AS (

  SELECT 
    CAST("BASKET LIST" AS STRING) AS "BASKET LIST",
    "PURCHASE YEAR" AS "PURCHASE YEAR",
    CAST(TREAT AS STRING) AS TREAT,
    CAST("STUFFED ANIMAL" AS STRING) AS "STUFFED ANIMAL"
  
  FROM TextInput_6 AS in0

),

Transpose_7 AS (

  {{
    prophecy_basics.Transpose(
      ['TextInput_6_cast'], 
      ['BASKET LIST', 'PURCHASE YEAR'], 
      ['TREAT', 'STUFFED ANIMAL'], 
      'NAME', 
      '"VALUE"', 
      ['BASKET LIST', 'PURCHASE YEAR', 'TREAT', 'STUFFED ANIMAL'], 
      true
    )
  }}

),

Join_8_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Transpose_7 AS in0
  INNER JOIN Union_4 AS in1
     ON (in0.VALUE = in1.TREAT)

),

Formula_11_0 AS (

  SELECT 
    (
      CASE
        WHEN ("PURCHASE YEAR" = "YEAR")
          THEN 'Yes'
        ELSE 'No '
      END
    ) AS FLAG,
    *
  
  FROM Join_8_inner AS in0

),

Filter_14 AS (

  SELECT * 
  
  FROM Formula_11_0 AS in0
  
  WHERE (FLAG = 'Yes')

),

Summarize_15 AS (

  SELECT 
    SUM(PRICE) AS "BASKET PRICE",
    "BASKET LIST" AS "BASKET LIST",
    "PURCHASE YEAR" AS "PURCHASE YEAR"
  
  FROM Filter_14 AS in0
  
  GROUP BY 
    "BASKET LIST", "PURCHASE YEAR"

),

MultiRowFormula_19_window AS (

  SELECT 
    *,
    LAG("BASKET PRICE", 1) OVER (PARTITION BY "BASKET LIST" ORDER BY "BASKET LIST" ASC NULLS FIRST) AS "BASKET PRICE_LAG1"
  
  FROM Summarize_15 AS in0

),

MultiRowFormula_19_0 AS (

  SELECT 
    ((("BASKET PRICE" - "BASKET PRICE_LAG1") / "BASKET PRICE_LAG1") * 100) AS "PERCENT INCREASE YOY",
    * EXCLUDE ("BASKET PRICE_LAG1")
  
  FROM MultiRowFormula_19_window AS in0

)

SELECT *

FROM MultiRowFormula_19_0
