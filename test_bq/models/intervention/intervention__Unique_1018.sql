{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH EDW_PROD_csv_1664 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'EDW_PROD_csv_1664_ref') }}

),

AlteryxSelect_998 AS (

  SELECT 
    FEP_Member_ID AS FEP_Member_ID,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM EDW_PROD_csv_1664 AS in0

),

HRHCReportLive__1679 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'HRHCReportLive__1679_ref') }}

),

AlteryxSelect_1000 AS (

  SELECT 
    NULL AS `FEP Member ID`,
    *
  
  FROM HRHCReportLive__1679 AS in0

),

Join_997_inner AS (

  SELECT 
    in1.MBR_INDV_BE_KEY AS Right_MBR_INDV_BE_KEY,
    in0.* EXCEPT (`FEP_Member_ID`),
    in1.* EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM AlteryxSelect_998 AS in0
  INNER JOIN AlteryxSelect_1000 AS in1
     ON (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)

),

AlteryxSelect_1002 AS (

  SELECT * EXCEPT (`FEP Member ID`, 
         `Name - Last`, 
         `Name - First`, 
         `Addr - Street1`, 
         `Addr - Street2`, 
         `Addr - City`, 
         `Addr - ZIP`, 
         `Subscriber Telephone`, 
         `Age Year Number`)
  
  FROM Join_997_inner AS in0

),

Formula_1006_0 AS (

  SELECT 
    'FEP' AS SOURCE_ID,
    *
  
  FROM AlteryxSelect_1002 AS in0

),

MultiFieldFormula_772 AS (

  SELECT *
  
  FROM {{ ref('intervention__MultiFieldFormula_772')}}

),

Join_1010_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Member Individual Business Entity Key` = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_INDV_BE_KEY
      END
    ) AS MBR_INDV_BE_KEY,
    in0.*,
    in1.* EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM MultiFieldFormula_772 AS in0
  FULL JOIN Formula_1006_0 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.MBR_INDV_BE_KEY)

),

Unique_1018 AS (

  SELECT * 
  
  FROM Join_1010_left_UnionFullOuter AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `Member Individual Business Entity Key`, 
  `Risk Cost Code`, 
  `Concurrent Normalized Risk Score`, 
  `Prospective Normalized Risk Score`, 
  `Recent 3 Months Total Allowable Amount`, 
  `Current 12 Months Total Allowable Amount`, 
  SOURCE_ID, 
  MBR_INDV_BE_KEY ORDER BY `Member Individual Business Entity Key`, `Risk Cost Code`, `Concurrent Normalized Risk Score`, `Prospective Normalized Risk Score`, `Recent 3 Months Total Allowable Amount`, `Current 12 Months Total Allowable Amount`, SOURCE_ID, MBR_INDV_BE_KEY) = 1

)

SELECT *

FROM Unique_1018
