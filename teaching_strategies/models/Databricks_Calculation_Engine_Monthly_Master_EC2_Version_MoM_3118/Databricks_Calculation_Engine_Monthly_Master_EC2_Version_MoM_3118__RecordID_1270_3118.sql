{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Filter_844_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_844_3118')}}

),

Filter_843_3118 AS (

  SELECT * 
  
  FROM Filter_844_3118 AS in0
  
  WHERE (
          (NOT(Stage IS NULL))
          AND (
                (NOT((UPPER(Origin) = UPPER('Orders&OrdersProcessed')) AND (`ARR Period` > StaticHistoryMonth)))
                OR (((UPPER(Origin) = UPPER('Orders&OrdersProcessed')) AND (`ARR Period` > StaticHistoryMonth)) IS NULL)
              )
        )

),

RecordID_1270_3118 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_843_3118'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

)

SELECT *

FROM RecordID_1270_3118
