{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_842 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_842')}}

),

RecordID_833 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__RecordID_833')}}

),

Join_823_inner AS (

  SELECT 
    in0.* EXCEPT (`RecordID`),
    in1.* EXCEPT (`RecordID`)
  
  FROM Summarize_842 AS in0
  INNER JOIN RecordID_833 AS in1
     ON (in0.RecordID = in1.RecordID)

),

Summarize_882 AS (

  SELECT 
    DISTINCT `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Order: Subscription Term` AS `Order: Subscription Term`
  
  FROM Join_823_inner AS in0

),

Summarize_883 AS (

  SELECT 
    SUM(CAST(`Order: Subscription Term` AS DECIMAL (19, 9))) AS `Updated Term`,
    `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`
  
  FROM Summarize_882 AS in0
  
  GROUP BY `Order: Account Name: Mas90 Customer Number`

),

Join_884_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Order: Account Name: Mas90 Customer Number`)
  
  FROM Join_823_inner AS in0
  INNER JOIN Summarize_883 AS in1
     ON (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Order: Account Name: Mas90 Customer Number`)

)

SELECT *

FROM Join_884_inner
