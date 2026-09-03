{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Tad_s2026RunRat_2485 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'Tad_s2026RunRat_2485'
    )
  }}

),

Filter_2486 AS (

  SELECT * 
  
  FROM Tad_s2026RunRat_2485 AS in0
  
  WHERE (NOT(F1 IS NULL))

),

DynamicRename_2487 AS (

  SELECT 
    F1 AS `QB Date`,
    F2 AS `Transaction Type`,
    F3 AS Num,
    F4 AS `Customer Name`,
    F5 AS `Memo/Description`,
    F6 AS Split,
    F7 AS Amount,
    F8 AS Stripe,
    F9 AS `Center Name`,
    F10 AS `SF Date`,
    F11 AS `Customer ID`,
    F12 AS Customer,
    F13 AS `Sale Date`,
    F14 AS `Start Date`,
    F15 AS `Expiration Date`,
    F16 AS `Order #`,
    F17 AS `SF Rev`,
    F18 AS variableDate,
    F19 AS Mas90,
    F20 AS `Customer Name2`,
    F21 AS Revenue,
    F22 AS Column1
  
  FROM Filter_2486 AS in0

),

DynamicRename_2487_row_number AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_2487'], 
      'incremental_id', 
      'prophecy_row_id', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

DynamicRename_2487_filter AS (

  SELECT * 
  
  FROM DynamicRename_2487_row_number AS in0
  
  WHERE (
          (
            NOT(
              prophecy_row_id = 1)
          ) OR (prophecy_row_id IS NULL)
        )

),

DynamicRename_2487_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM DynamicRename_2487_filter AS in0

),

AlteryxSelect_2490 AS (

  SELECT * EXCEPT (`Transaction Type`, 
         `Num`, 
         `Customer Name`, 
         `Memo/Description`, 
         `Split`, 
         `Amount`, 
         `Stripe`, 
         `Center Name`, 
         `SF Date`, 
         `Customer ID`, 
         `Customer`, 
         `Sale Date`, 
         `Start Date`, 
         `Expiration Date`, 
         `Order #`, 
         `SF Rev`, 
         `Column1`)
  
  FROM DynamicRename_2487_drop_0 AS in0

)

SELECT *

FROM AlteryxSelect_2490
