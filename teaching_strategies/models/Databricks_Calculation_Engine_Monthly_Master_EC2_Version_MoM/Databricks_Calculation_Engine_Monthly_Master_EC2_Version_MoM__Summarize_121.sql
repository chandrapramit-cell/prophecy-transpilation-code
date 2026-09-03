{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH DSN_Databricks__3340 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DSN_Databricks__3340'
    )
  }}

),

AlteryxSelect_3341 AS (

  SELECT 
    ACCOUNT_MERGE_HISTORY_NAME AS `Account Merge History Name`,
    CREATED_BY_FULLNAME AS `Created By: Full Name`,
    CREATED_DATE AS `Created Date`,
    WINNER_ACCOUNT_MAS90_CUSTOMER_NUMBER AS `Winner Account: Mas90 Customer Number`,
    WINNER_ACCOUNT_ACCOUNT_NAME AS `Winner Account: Account Name`,
    WINNER_MAS90_CUSTOMER_NUMBER AS `Winner Mas90 Customer Number`,
    LOSER_ACCOUNT_ID AS `Loser Account Id`,
    LOSER_MAS90_CUSTOMER_NUMBER AS `Loser Mas90 Customer Number`,
    LOSER_NAME AS `Loser Name`,
    * EXCEPT (`account_merge_history_name`, 
    `created_by_fullname`, 
    `created_date`, 
    `winner_account_mas90_customer_number`, 
    `winner_account_account_name`, 
    `winner_mas90_customer_number`, 
    `loser_account_id`, 
    `loser_mas90_customer_number`, 
    `loser_name`)
  
  FROM DSN_Databricks__3340 AS in0

),

Summarize_121 AS (

  SELECT 
    DISTINCT `Loser Mas90 Customer Number` AS `Loser Mas90 Customer Number`,
    `Winner Account: Mas90 Customer Number` AS `Winner Mas90 Customer Number`
  
  FROM AlteryxSelect_3341 AS in0

)

SELECT *

FROM Summarize_121
