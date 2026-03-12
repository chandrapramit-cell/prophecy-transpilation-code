{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_1')}}

),

TextInput_1_cast AS (

  SELECT 
    ID AS ID,
    CAST(Name AS STRING) AS Name,
    Q1_Sales AS Q1_Sales,
    Q1_Profit AS Q1_Profit,
    Q2_Sales AS Q2_Sales,
    Q2_Profit AS Q2_Profit
  
  FROM TextInput_1 AS in0

),

Arrange_2_consolidatedDataCol_0 AS (

  {#Compiles a consolidated data column with an empty placeholder alongside input data for downstream processing.#}
  SELECT 
    [] AS _consolidated_data_col,
    *
  
  FROM TextInput_1_cast AS in0

),

Arrange_2_explode AS (

  SELECT * 
  
  FROM Arrange_2_consolidatedDataCol_0 AS in0
  LEFT JOIN UNNEST(`_consolidated_data_col`) AS _exploded_data_col

),

Arrange_2 AS (

  SELECT *
  
  FROM Arrange_2_explode

),

Arrange_2_selectCols AS (

  SELECT * 
  
  FROM Arrange_2 AS in0

)

SELECT *

FROM Arrange_2_selectCols
