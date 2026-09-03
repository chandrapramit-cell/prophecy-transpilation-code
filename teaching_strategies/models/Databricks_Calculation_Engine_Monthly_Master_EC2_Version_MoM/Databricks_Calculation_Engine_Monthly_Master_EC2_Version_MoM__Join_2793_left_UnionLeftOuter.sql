{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_2786_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2786_left_UnionLeftOuter')}}

),

Summarize_2780 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2780')}}

),

Filter_2807 AS (

  SELECT * 
  
  FROM Join_2786_left_UnionLeftOuter AS in0
  
  WHERE ((RevMonth >= First_NonZeroRevMonth) OR (to_date(`Previous Period`) <= Last_NonZeroRevMonth))

),

Summarize_2779 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2779')}}

),

Join_2792_inner AS (

  SELECT 
    in0.Revenue AS `Initial Revenue`,
    in0.* EXCEPT (`RevMonth`, `Revenue`),
    in1.* EXCEPT (`CustomerName`, `First_PosRevMonth`, `Last_PosRevMonth`)
  
  FROM Summarize_2780 AS in0
  INNER JOIN Summarize_2779 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.RevMonth = in1.First_PosRevMonth))

),

Join_2793_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CustomerName`, 
    `Customer Segment`, 
    `SubCustSeg1`, 
    `SubCustSeg2`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg5`, 
    `SubCustSeg6`, 
    `Volume`, 
    `YetToRenew`)
  
  FROM Filter_2807 AS in0
  LEFT JOIN Join_2792_inner AS in1
     ON (in0.CustomerName = in1.CustomerName)

)

SELECT *

FROM Join_2793_left_UnionLeftOuter
