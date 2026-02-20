{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TOTAL_INF_CONDI_1662 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'TOTAL_INF_CONDI_1662_ref') }}

),

AlteryxSelect_1635 AS (

  SELECT 
    `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`,
    `Mara Common Chronic Condition` AS `Mara Common Chronic Condition`
  
  FROM TOTAL_INF_CONDI_1662 AS in0

),

Summarize_1642 AS (

  SELECT 
    DISTINCT `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`,
    `Mara Common Chronic Condition` AS `Mara Common Chronic Condition`
  
  FROM AlteryxSelect_1635 AS in0

),

ProviderDetailR_1661 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'ProviderDetailR_1661_ref') }}

),

Filter_1638 AS (

  SELECT * 
  
  FROM ProviderDetailR_1661 AS in0
  
  WHERE (`Recapture Flag` = 'Not Captured')

),

AlteryxSelect_1639 AS (

  SELECT `Member Individual BE Key` AS `Member Individual Business Entity Key`
  
  FROM Filter_1638 AS in0

),

Summarize_1640 AS (

  SELECT DISTINCT `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`
  
  FROM AlteryxSelect_1639 AS in0

),

Union_1641 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_1640', 'Summarize_1642'], 
      [
        '[{"name": "Member Individual Business Entity Key", "dataType": "String"}]', 
        '[{"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "Mara Common Chronic Condition", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_1643 AS (

  SELECT 
    DISTINCT `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`,
    `Mara Common Chronic Condition` AS `Mara Common Chronic Condition`
  
  FROM Union_1641 AS in0

),

Formula_847_0 AS (

  SELECT 
    1 AS `MARA INDICATOR`,
    *
  
  FROM Summarize_1643 AS in0

),

CrossTab_846 AS (

  SELECT *
  
  FROM (
    SELECT 
      `Member Individual Business Entity Key`,
      `Mara Common Chronic Condition`,
      `MARA INDICATOR`
    
    FROM Formula_847_0 AS in0
  )
  PIVOT (
    SUM(`MARA INDICATOR`) AS Sum
    FOR `Mara Common Chronic Condition`
    IN (
      'CAD', 'Hypertension', 'ESRD', 'Cancer', 'CHF', '_Null_', 'Diabetes', 'COPD', 'Asthma'
    )
  )

),

selectdistinct__1663 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'selectdistinct__1663_ref') }}

),

Summarize_851 AS (

  SELECT 
    COUNT(DISTINCT CLM_ID) AS CountDistinct_CLM_ID,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM selectdistinct__1663 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

AlteryxSelect_852 AS (

  select  CountDistinct_CLM_ID as `Congestive_Heart_Failure` , *  REPLACE( CAST(MBR_INDV_BE_KEY AS STRING) as `MBR_INDV_BE_KEY` ) from Summarize_851

),

Join_850_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Member Individual Business Entity Key` = in1.MBR_INDV_BE_KEY)
          THEN in1.Congestive_Heart_Failure
        ELSE NULL
      END
    ) AS Right_Congestive_Heart_Failure,
    in0.*
  
  FROM CrossTab_846 AS in0
  LEFT JOIN AlteryxSelect_852 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.MBR_INDV_BE_KEY)

)

SELECT *

FROM Join_850_left_UnionLeftOuter
