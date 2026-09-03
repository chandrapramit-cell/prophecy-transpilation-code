{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_884_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_884_inner')}}

),

Formula_708_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_708_0')}}

),

Join_700_inner AS (

  SELECT 
    in1.StartDate_Annualization AS Right_StartDate_Annualization,
    in1.EndDate_Annualization AS Right_EndDate_Annualization,
    in1.TCV AS Right_TCV,
    in1.`Updated Term` AS `Right_Updated Term`,
    in1.Engine_ContractDays AS Right_Engine_ContractDays,
    in1.Product AS Right_Product,
    in0.*,
    in1.* EXCEPT (`StartDate_Annualization`, `EndDate_Annualization`, `TCV`, `Updated Term`, `Engine_ContractDays`, `Product`)
  
  FROM Join_884_inner AS in0
  INNER JOIN Formula_708_0 AS in1
     ON (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Mas90 Customer Number`)

)

SELECT *

FROM Join_700_inner
