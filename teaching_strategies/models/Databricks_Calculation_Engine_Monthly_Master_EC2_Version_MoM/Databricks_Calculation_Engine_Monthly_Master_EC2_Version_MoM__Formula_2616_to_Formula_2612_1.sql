{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_1199 AS (

  SELECT SUM(REVENUE) AS Sum_Revenue
  
  FROM `` AS in0

),

AlteryxSelect_2609 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2609')}}

),

Summarize_2610 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    `Customer Level Flag` AS `Customer Level Flag`
  
  FROM AlteryxSelect_2609 AS in0
  
  GROUP BY `Customer Level Flag`

),

AppendFields_2611 AS (

  SELECT in0.Sum_Revenue AS Source_Sum_Revenue
  
  FROM Summarize_1199 AS in0
  INNER JOIN Summarize_2610 AS in1
     ON true

),

Formula_2616_to_Formula_2612_0 AS (

  SELECT 
    CAST((
      (
        CASE
          WHEN (((Sum_Revenue / 0.01) < 0) AND (((Sum_Revenue / 0.01) - FLOOR((Sum_Revenue / 0.01))) = 0.5))
            THEN CEIL((Sum_Revenue / 0.01))
          ELSE ROUND((Sum_Revenue / 0.01))
        END
      )
      * 0.01
    ) AS DOUBLE) AS Sum_Revenue,
    * EXCEPT (`sum_revenue`)
  
  FROM AppendFields_2611 AS in0

),

Formula_2616_to_Formula_2612_1 AS (

  SELECT 
    CAST(CASE
      WHEN (Sum_Revenue = Source_Sum_Revenue)
        THEN 'PASS'
      ELSE 'FAIL'
    END AS STRING) AS `Check`,
    *
  
  FROM Formula_2616_to_Formula_2612_0 AS in0

)

SELECT *

FROM Formula_2616_to_Formula_2612_1
