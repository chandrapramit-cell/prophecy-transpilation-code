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

Filter_3079_reject AS (

  SELECT * 
  
  FROM Union_677 AS in0
  
  WHERE (
          NOT (
            (
              (
                (`Order: Business Subtype` IN ('New', 'New-ATE'))
                AND (to_date(array_min(array(`Order: Opportunity: Actual Closed Date`, `Order: Start Date`))) = `Order: Opportunity: Actual Closed Date`)
              )
              AND (
                    (
                      (
                        NOT(
                          `Order: Opportunity: Actual Closed Date` = to_date(`Order: Start Date`))
                      )
                      OR isnull(`Order: Opportunity: Actual Closed Date`)
                    )
                    OR isnull(`Order: Start Date`)
                  )
            )
          )
          OR isnull(
               (
                 (
                   (`Order: Business Subtype` IN ('New', 'New-ATE'))
                   AND (to_date(array_min(array(`Order: Opportunity: Actual Closed Date`, `Order: Start Date`))) = `Order: Opportunity: Actual Closed Date`)
                 )
                 AND (
                       (
                         (
                           NOT(
                             `Order: Opportunity: Actual Closed Date` = to_date(`Order: Start Date`))
                         )
                         OR isnull(`Order: Opportunity: Actual Closed Date`)
                       )
                       OR isnull(`Order: Start Date`)
                     )
               ))
        )

)

SELECT *

FROM Filter_3079_reject
