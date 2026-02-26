{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TextInput_337 AS (

  SELECT * 
  
  FROM {{ ref('seed_337')}}

),

TextInput_337_cast AS (

  SELECT 
    CAST(Variable AS STRING) AS Variable,
    CAST(`Starting Position` AS INTEGER) AS `Starting Position`,
    CAST(Length AS INTEGER) AS Length
  
  FROM TextInput_337 AS in0

),

DynamicInput_617 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_617') }}

),

DynamicInput_614 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_614') }}

),

Union_396 AS (

  {{
    prophecy_basics.UnionByName(
      ['DynamicInput_614', 'DynamicInput_617'], 
      [
        '[{"name": "Field_1", "dataType": "String"}, {"name": "FileName", "dataType": "String"}]', 
        '[{"name": "Field_1", "dataType": "String"}, {"name": "FileName", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_336 AS (

  SELECT 
    Field_1 AS RAW_DATA,
    FileName AS FILENAME,
    * EXCEPT (`FILENAME`, `Field_1`)
  
  FROM Union_396 AS in0

),

RecordID_342 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM AlteryxSelect_336

),

AppendFields_338 AS (

  SELECT 
    in0.Variable AS Variable,
    in0.* EXCEPT (`Variable`),
    in1.*
  
  FROM TextInput_337_cast AS in0
  INNER JOIN RecordID_342 AS in1
     ON TRUE

),

Formula_340_0 AS (

  SELECT 
    CAST((SUBSTRING(RAW_DATA, ((`Starting Position` - 1) + 1), Length)) AS STRING) AS `Value`,
    *
  
  FROM AppendFields_338 AS in0

),

CrossTab_341 AS (

  SELECT *
  
  FROM (
    SELECT 
      RecordID,
      FILENAME,
      Variable,
      Value
    
    FROM Formula_340_0 AS in0
  )
  PIVOT (
    FIRST(Value) AS First
    FOR Variable
    IN (
      'v_Sign', 'v_PremiumClass', 'v_Reinsurer', 'v_Value', 'v_Period', 'v_Measure', 'v_Company', 'v_ProductCode'
    )
  )

),

Formula_346_to_Formula_350_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (v_Sign = '-')
          THEN ((CAST(v_Value AS INTEGER) / 100) * -1)
        ELSE (CAST(v_Value AS INTEGER) / 100)
      END
    ) AS DOUBLE) AS n_Value,
    CAST((
      CASE
        WHEN (v_ProductCode = 'ZZZ')
          THEN 'None'
        ELSE v_ProductCode
      END
    ) AS STRING) AS v_ProductCode,
    CAST((
      CASE
        WHEN (v_PremiumClass = 'ZZZ')
          THEN 'None'
        ELSE v_PremiumClass
      END
    ) AS STRING) AS v_PremiumClass,
    * EXCEPT (`v_premiumclass`, `v_productcode`)
  
  FROM CrossTab_341 AS in0

),

CrossTab_353 AS (

  SELECT *
  
  FROM (
    SELECT 
      v_Company,
      v_Period,
      v_PremiumClass,
      v_ProductCode,
      v_Reinsurer,
      v_Measure,
      n_Value
    
    FROM Formula_346_to_Formula_350_0 AS in0
  )
  PIVOT (
    SUM(n_Value) AS Sum
    FOR v_Measure
    IN (
      'RUP', 'RUA'
    )
  )

),

AlteryxSelect_354 AS (

  SELECT 
    v_Period AS v_Period,
    v_Company AS v_Company,
    v_Reinsurer AS v_Reinsurer,
    v_ProductCode AS v_ProductCode,
    v_PremiumClass AS v_PremiumClass,
    RUP AS RUP,
    RUA AS RUA
  
  FROM CrossTab_353 AS in0

),

Filter_631 AS (

  SELECT * 
  
  FROM AlteryxSelect_354 AS in0
  
  WHERE not(contains(v_Reinsurer, 'DUMMY'))

)

SELECT *

FROM Filter_631
