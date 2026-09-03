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

AlteryxSelect_1894 AS (

  SELECT 
    StartDate_Annualization AS ContractStartDate,
    EndDate_Annualization AS ContractEndDate,
    * EXCEPT (`Order: Order`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    `Order: Activated Date`, 
    `Order: Start Date`, 
    `Order: End Date (Calculated)`, 
    `Expected Renewal Date`, 
    `StartDate_Annualization`, 
    `EndDate_Annualization`)
  
  FROM Formula_1893_0 AS in0

)

SELECT *

FROM AlteryxSelect_1894
