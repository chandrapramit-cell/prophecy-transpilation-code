{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Filter_863_3118_to_Filter_870_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_863_3118_to_Filter_870_3118')}}

),

Filter_875_3118_reject AS (

  SELECT * 
  
  FROM Filter_863_3118_to_Filter_870_3118 AS in0
  
  WHERE (
          (
            (
              NOT(
                UPPER(Origin) = UPPER('Renewals'))
            ) OR (Origin IS NULL)
          )
          OR ((UPPER(Origin) = UPPER('Renewals')) IS NULL)
        )

)

SELECT *

FROM Filter_875_3118_reject
