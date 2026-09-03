{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH ManualAdjustmen_564 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'ManualAdjustmen_564'
    )
  }}

),

Filter_825 AS (

  SELECT * 
  
  FROM ManualAdjustmen_564 AS in0
  
  WHERE TRUE

),

AlteryxSelect_2607 AS (

  SELECT 
    `  Customer Data ARR  ` AS `Customer Data ARR`,
    * EXCEPT (`variableKey`, `  Customer Data ARR  `)
  
  FROM Filter_825 AS in0

),

AlteryxSelect_1176 AS (

  SELECT 
    Modified_Start_date AS ContractStartDate,
    EndDate_Annualization AS ContractEndDate,
    * EXCEPT (`Modified_Start_date`, `EndDate_Annualization`)
  
  FROM AlteryxSelect_2607 AS in0

),

Formula_1175_0 AS (

  SELECT 
    CAST(CASE
      WHEN ((ContractStartDate <= to_date('2016-02-28')) AND (ContractEndDate >= to_date('2016-02-29')))
        THEN CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT)
      WHEN ((ContractStartDate <= to_date('2020-02-28')) AND (ContractEndDate >= to_date('2020-02-29')))
        THEN CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT)
      WHEN ((ContractStartDate <= to_date('2024-02-28')) AND (ContractEndDate >= to_date('2024-02-29')))
        THEN CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT)
      WHEN ((ContractStartDate <= to_date('2028-02-28')) AND (ContractEndDate >= to_date('2028-02-29')))
        THEN CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT)
      ELSE (CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT) + 1)
    END AS DOUBLE) AS ContractTermDays,
    *
  
  FROM AlteryxSelect_1176 AS in0

),

Formula_1175_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (ContractTermDays <= 0)
          THEN 0
        ELSE ((`Customer Data ARR` * ContractTermDays) / 365.25)
      END
    ) AS DOUBLE) AS TCV,
    *
  
  FROM Formula_1175_0 AS in0

),

AlteryxSelect_565 AS (

  SELECT 
    `Customer Name` AS `Mas90 Customer Number`,
    ContractStartDate AS StartDate_Annualization,
    ContractEndDate AS EndDate_Annualization,
    * EXCEPT (`Year`, `Customer Name`, `ContractStartDate`, `ContractEndDate`)
  
  FROM Formula_1175_1 AS in0

),

RecordID_3126 AS (

  {{
    prophecy_basics.RecordID(
      ['AlteryxSelect_565'], 
      'incremental_id', 
      'ManualRecordID', 
      'integer', 
      6, 
      1, 
      'groupLevel', 
      'first_column', 
      [], 
      [{ 'expression': { 'expression': 'prophecy_recordId_564' }, 'sortType': 'asc' }]
    )
  }}

),

Formula_3125_0 AS (

  SELECT 
    CAST('Manual Plug In' AS string) AS Origin,
    *
  
  FROM RecordID_3126 AS in0

)

SELECT *

FROM Formula_3125_0
