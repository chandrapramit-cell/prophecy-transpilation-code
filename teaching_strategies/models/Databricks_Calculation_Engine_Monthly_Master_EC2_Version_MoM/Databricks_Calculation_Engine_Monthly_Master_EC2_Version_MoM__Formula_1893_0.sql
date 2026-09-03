{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH MultiFieldFormula_1895 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__MultiFieldFormula_1895')}}

),

Formula_1893_0 AS (

  SELECT 
    last_day(
      CASE
        WHEN (
          (
            (CAST(Origin AS STRING) IN ('Manual Plug In', 'Orders&OrdersProcessed - Fall 2020 Early Renewals'))
            OR coalesce(contains(lower(Stage), lower('Closed')), false)
          )
          AND (StartDate_Annualization < `Actual Closed Date`)
        )
          THEN StartDate_Annualization
        WHEN CAST(isnull(`Actual Closed Date`) AS BOOLEAN)
          THEN `Created Date`
        ELSE `Actual Closed Date`
      END) AS `ARR Period`,
    CAST((
      CASE
        WHEN CAST((Quantity IS NULL) AS BOOLEAN)
          THEN 0
        WHEN ((Quantity IS NULL) OR ((LENGTH(CAST(Quantity AS string))) = 0))
          THEN 0
        ELSE Quantity
      END
    ) AS DOUBLE) AS Quantity,
    * EXCEPT (`quantity`)
  
  FROM MultiFieldFormula_1895 AS in0

)

SELECT *

FROM Formula_1893_0
