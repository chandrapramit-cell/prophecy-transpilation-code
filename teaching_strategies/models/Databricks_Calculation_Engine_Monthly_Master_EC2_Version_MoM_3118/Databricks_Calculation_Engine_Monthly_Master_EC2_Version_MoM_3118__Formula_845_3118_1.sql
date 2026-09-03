{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH RecordID_1270_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__RecordID_1270_3118')}}

),

Formula_845_3118_0 AS (

  SELECT 
    CAST(CASE
      WHEN (
        (to_date(last_day(`Actual Closed Date`)) <= StaticHistoryMonth)
        OR (to_date(last_day(`Created Date`)) > StaticHistoryMonth)
      )
        THEN 0
      ELSE 1
    END AS BOOLEAN) AS `Open Renewal Flag`,
    *
  
  FROM RecordID_1270_3118 AS in0

),

Formula_845_3118_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`Open Renewal Flag` = FALSE)
          THEN 0
        ELSE TCV
      END
    ) AS DOUBLE) AS TCV,
    * EXCEPT (`tcv`)
  
  FROM Formula_845_3118_0 AS in0

)

SELECT *

FROM Formula_845_3118_1
