{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_156_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_156_3124')}}

),

Formula_172_3124_0 AS (

  SELECT 
    CAST(CASE
      WHEN (
        (StartDate_Annualization <= to_date('2023-12-31'))
        AND (EndDate_Annualization >= to_date('2023-12-31'))
      )
        THEN (
          (TCV / CAST(datediff(to_date(EndDate_Annualization), to_date(StartDate_Annualization)) AS INT))
          * 365.25
        )
      ELSE 0
    END AS DOUBLE) AS `Dec-23`,
    *
  
  FROM Summarize_156_3124 AS in0

),

Summarize_173_3124 AS (

  SELECT SUM(`Dec-23`) AS `Sum_Dec-23`
  
  FROM Formula_172_3124_0 AS in0

)

SELECT *

FROM Summarize_173_3124
