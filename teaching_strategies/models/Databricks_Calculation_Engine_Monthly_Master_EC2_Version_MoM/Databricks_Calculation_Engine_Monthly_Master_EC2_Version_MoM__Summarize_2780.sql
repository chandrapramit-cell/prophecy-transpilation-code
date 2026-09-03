{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_1280 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1280')}}

),

Summarize_2780 AS (

  SELECT 
    SUM(Revenue) AS Revenue,
    SUM(Volume) AS Volume,
    SUM(YetToRenew) AS YetToRenew,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg1 AS SubCustSeg1,
    CustomerName AS CustomerName,
    SubCustSeg6 AS SubCustSeg6,
    `Customer Segment` AS `Customer Segment`,
    RevMonth AS RevMonth,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    SubCustSeg2 AS SubCustSeg2
  
  FROM Formula_1280 AS in0
  
  GROUP BY 
    SubCustSeg5, 
    SubCustSeg1, 
    CustomerName, 
    SubCustSeg6, 
    `Customer Segment`, 
    RevMonth, 
    SubCustSeg3, 
    SubCustSeg4, 
    SubCustSeg2

)

SELECT *

FROM Summarize_2780
