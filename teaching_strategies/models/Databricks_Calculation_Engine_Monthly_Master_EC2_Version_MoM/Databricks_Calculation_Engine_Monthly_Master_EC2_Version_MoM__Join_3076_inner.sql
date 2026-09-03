{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_1893_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1893_0')}}

),

MultiFieldFormula_1895 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__MultiFieldFormula_1895')}}

),

Formula_3073_0 AS (

  SELECT 
    (
      LAST_DAY(
        CAST((
          CASE
            WHEN CAST((CAST(Origin AS string) IN ('Manual Plug In', 'Orders&OrdersProcessed - Fall 2020 Early Renewals')) AS BOOLEAN)
              THEN CAST(StartDate_Annualization AS string)
            WHEN CAST((`Order: Activated Date` IS NULL) AS BOOLEAN)
              THEN CAST(`Expected Renewal Date` AS string)
            ELSE `Order: Activated Date`
          END
        ) AS DATE))
    ) AS `ARR Period`,
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

),

Summarize_3077 AS (

  SELECT 
    DISTINCT RecordID AS RecordID,
    `ARR Period` AS `ARR Period`
  
  FROM Formula_3073_0 AS in0

),

Summarize_3075 AS (

  SELECT 
    DISTINCT RecordID AS RecordID,
    `ARR Period` AS `ARR Period`
  
  FROM Formula_1893_0 AS in0

),

Join_3076_inner AS (

  SELECT 
    in1.RecordID AS Right_RecordID,
    in1.`ARR Period` AS `Right_ARR Period`,
    in0.*,
    in1.* EXCEPT (`RecordID`, `ARR Period`)
  
  FROM Summarize_3075 AS in0
  INNER JOIN Summarize_3077 AS in1
     ON ((in0.RecordID = in1.RecordID) AND (in0.`ARR Period` = in1.`ARR Period`))

)

SELECT *

FROM Join_3076_inner
