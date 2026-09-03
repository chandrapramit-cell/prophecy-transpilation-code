{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_121 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_121')}}

),

AlteryxSelect_3090 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3090')}}

),

Join_3162_left AS (

  SELECT in0.*
  
  FROM Summarize_121 AS in0
  ANTI JOIN AlteryxSelect_3090 AS in1
     ON (in0.`Loser Mas90 Customer Number` = in1.`Loser Mas90 Customer Number`)

),

Union_3091 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_3090', 'Join_3162_left'], 
      [
        '[{"name": "Loser Mas90 Customer Number", "dataType": "String"}, {"name": "Winner Mas90 Customer Number", "dataType": "String"}]', 
        '[{"name": "Loser Mas90 Customer Number", "dataType": "String"}, {"name": "Winner Mas90 Customer Number", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_122 AS (

  SELECT * 
  
  FROM Union_3091 AS in0
  
  WHERE (
          (
            (NOT((`Loser Mas90 Customer Number` IS NULL) OR ((LENGTH(`Loser Mas90 Customer Number`)) = 0)))
            AND (NOT((`Winner Mas90 Customer Number` IS NULL) OR ((LENGTH(`Winner Mas90 Customer Number`)) = 0)))
          )
          AND (
                (
                  (
                    NOT(
                      UPPER(`Winner Mas90 Customer Number`) = UPPER(`Loser Mas90 Customer Number`))
                  )
                  OR (`Winner Mas90 Customer Number` IS NULL)
                )
                OR (`Loser Mas90 Customer Number` IS NULL)
              )
        )

),

Unique_123 AS (

  SELECT * 
  
  FROM Filter_122 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `Loser Mas90 Customer Number` ORDER BY `Loser Mas90 Customer Number`) = 1

)

SELECT *

FROM Unique_123
