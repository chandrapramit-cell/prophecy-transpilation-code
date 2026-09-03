{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_1067 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1067')}}

),

Filter_3260 AS (

  SELECT * 
  
  FROM Summarize_1067 AS in0
  
  WHERE not(contains(Stage, 'Close'))

),

Formula_3261_0 AS (

  SELECT 
    CAST(UPPER(`Product Code`) AS string) AS `Product Code`,
    * EXCEPT (`product code`)
  
  FROM Filter_3260 AS in0

),

AlteryxSelect_3310 AS (

  SELECT 
    StartDate_Annualization AS ContractStartDate,
    EndDate_Annualization AS ContractEndDate,
    * EXCEPT (`StartDate_Annualization`, `EndDate_Annualization`)
  
  FROM Formula_3261_0 AS in0

),

Filter_3311 AS (

  SELECT * 
  
  FROM AlteryxSelect_3310 AS in0
  
  WHERE ((NOT(ContractStartDate IS NULL)) AND (NOT(ContractEndDate IS NULL)))

),

RecordID_3308 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_3311'], 
      'incremental_id', 
      'RecordID', 
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

Formula_3309_to_Formula_3314_0 AS (

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
  
  FROM RecordID_3308 AS in0

),

Formula_3309_to_Formula_3314_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (ContractTermDays <= 0)
          THEN 0
        ELSE (
          (
            CASE
              WHEN (
                ((((TCV / ContractTermDays) * 365.25) / 0.01) < 0)
                AND (((((TCV / ContractTermDays) * 365.25) / 0.01) - FLOOR((((TCV / ContractTermDays) * 365.25) / 0.01))) = 0.5)
              )
                THEN CEIL((((TCV / ContractTermDays) * 365.25) / 0.01))
              ELSE ROUND((((TCV / ContractTermDays) * 365.25) / 0.01))
            END
          )
          * 0.01
        )
      END
    ) AS DOUBLE) AS ARR,
    CAST((
      (
        CASE
          WHEN (
            (((ContractTermDays / 30.4375) / 0.1) < 0)
            AND ((((ContractTermDays / 30.4375) / 0.1) - FLOOR(((ContractTermDays / 30.4375) / 0.1))) = 0.5)
          )
            THEN CEIL(((ContractTermDays / 30.4375) / 0.1))
          ELSE ROUND(((ContractTermDays / 30.4375) / 0.1))
        END
      )
      * 0.1
    ) AS DOUBLE) AS ContractTermMonths,
    CAST((SUBSTRING(CAST(`Expected Renewal Date` AS string), 1, 4)) AS string) AS `Expected Year Renewals`,
    CAST(UPPER(`Product Code`) AS string) AS `Product Code`,
    CAST((SUBSTRING(CAST(ContractStartDate AS string), 1, 4)) AS string) AS `Expected Year Renewals Quote`,
    * EXCEPT (`product code`)
  
  FROM Formula_3309_to_Formula_3314_0 AS in0

),

Filter_3315 AS (

  SELECT * 
  
  FROM Formula_3309_to_Formula_3314_1 AS in0
  
  WHERE (`Expected Year Renewals` < `Next Year`)

)

SELECT *

FROM Filter_3315
