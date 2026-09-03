{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Union_677 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_677')}}

),

Filter_696 AS (

  SELECT * 
  
  FROM Union_677 AS in0
  
  WHERE (
          (
            ((UPPER(Product) = UPPER('CC Cloud')) AND (`Order: Activated Date` >= '2020-06-01'))
            AND (`Order: Activated Date` <= '2020-12-31')
          )
          AND (
                (
                  NOT(
                    `Sum_Total Price (new)` = 0)
                ) OR (`Sum_Total Price (new)` IS NULL)
              )
        )

),

RecordID_833 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_696'], 
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

FROM RecordID_833
