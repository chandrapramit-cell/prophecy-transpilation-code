{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH DSN_Databricks__3343 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DSN_Databricks__3343'
    )
  }}

),

AlteryxSelect_3345 AS (

  SELECT 
    CAST(Acquired_Arr_From_Readyrosie AS DOUBLE) AS acquired_arr_from_readyrosie,
    CAST(Acquired_Arr_From_Quorum_Qualityassist AS DOUBLE) AS acquired_arr_from_quorum_qualityassist,
    * EXCEPT (`acquired_arr_from_readyrosie`, `acquired_arr_from_quorum_qualityassist`)
  
  FROM DSN_Databricks__3343 AS in0

),

Formula_3346_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (acquired_arr_from_readyrosie < 0)
          THEN 0
        ELSE acquired_arr_from_readyrosie
      END
    ) AS DOUBLE) AS acquired_arr_from_readyrosie,
    CAST((
      CASE
        WHEN (acquired_arr_from_quorum_qualityassist < 0)
          THEN 0
        ELSE acquired_arr_from_quorum_qualityassist
      END
    ) AS DOUBLE) AS acquired_arr_from_quorum_qualityassist,
    * EXCEPT (`acquired_arr_from_readyrosie`, `acquired_arr_from_quorum_qualityassist`)
  
  FROM AlteryxSelect_3345 AS in0

),

AlteryxSelect_3342 AS (

  SELECT 
    Mass90_Customer_Number AS `Mas90 Customer Number`,
    Sector AS Sector,
    Territory_Region AS `Territory Name`,
    variableType AS variableType,
    BillingState AS State,
    Partner_Success_Owner AS `Partner Success Owner`,
    Account_Owner AS `Account Owner`,
    acquired_arr_from_readyrosie AS `Acquired ARR from ReadyRosie`,
    acquired_arr_from_quorum_qualityassist AS `Acquired ARR from Quorum (QualityAssist)`,
    Date_Of_First_Activated_Order AS `Date of First Activated Order`,
    Date_Of_First_Active_Subscription_Order AS `Date of First Active Subscription Order`
  
  FROM Formula_3346_0 AS in0

)

SELECT *

FROM AlteryxSelect_3342
