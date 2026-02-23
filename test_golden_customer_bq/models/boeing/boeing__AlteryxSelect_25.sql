{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_68 AS (

  SELECT *
  
  FROM {{ ref('boeing__Filter_68')}}

),

Database__REPOR_36 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__REPOR_36_ref') }}

),

AlteryxSelect_9 AS (

  SELECT 
    OBS_LF AS OBS_LF,
    * EXCEPT (`OBS_LF`)
  
  FROM Database__REPOR_36 AS in0

),

Database__Repor_8 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__Repor_8_ref') }}

),

Formula_37_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (OPER_CARR_AIRLINE_CODE = 'AS')
          THEN 'ML'
        ELSE 'RG'
      END
    ) AS STRING) AS FLEET,
    (PYLD_PAX / CAP) AS REV_LF,
    *
  
  FROM Database__Repor_8 AS in0

),

Join_11_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`FLEET`, `EQUIP`, `IATA_EQUIP`, `CABIN`, `K_FACTOR`, `OBS_PAX`, `OBS_LF`, `CAP`, `DMD`, `UNCONSTRAINED_LF`, `REFRESHED`)
  
  FROM Formula_37_0 AS in0
  INNER JOIN AlteryxSelect_9 AS in1
     ON (
      (((t.FLEET = in1.FLEET) AND (t.REV_LF0 = in1.OBS_LF)) AND (t.CABIN = in1.CABIN))
      AND (t.IATA_CODE = in1.IATA_EQUIP)
    )

),

Join_11_left AS (

  SELECT in0.*
  
  FROM Formula_37_0 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.FLEET,
      in1.OBS_LF,
      in1.CABIN,
      in1.IATA_EQUIP
    
    FROM AlteryxSelect_9 AS in1
    
    WHERE in1.FLEET IS NOT NULL
          AND in1.OBS_LF IS NOT NULL
          AND in1.CABIN IS NOT NULL
          AND in1.IATA_EQUIP IS NOT NULL
  ) AS in1_keys
     ON (
      (((t.FLEET = in1_keys.FLEET) AND (t.REV_LF0 = in1_keys.OBS_LF)) AND (t.CABIN = in1_keys.CABIN))
      AND (t.IATA_CODE = in1_keys.IATA_EQUIP)
    )
  
  WHERE (in1_keys.FLEET IS NULL)

),

Filter_92 AS (

  SELECT * 
  
  FROM AlteryxSelect_9 AS in0
  
  WHERE (EQUIP = '737-800')

),

Join_93_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (((in0.FLEET = in1.FLEET) AND (CAST(in0.REV_LF AS FLOAT64) = in1.OBS_LF)) AND (in0.CABIN = in1.CABIN))
          THEN NULL
        ELSE 0
      END
    ) AS SPILL_PROBABILITY,
    in0.* EXCEPT (`FLEET`, `CABIN`, `CAP`),
    in1.* EXCEPT (`SPILL_PROBABILITY`)
  
  FROM Join_11_left AS in0
  LEFT JOIN Filter_92 AS in1
     ON (((t.FLEET = in1.FLEET) AND (t.REV_LF0 = in1.OBS_LF)) AND (t.CABIN = in1.CABIN))

),

Union_95 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_11_inner', 'Join_93_inner_UnionLeftOuter'], 
      [
        '[{"name": "SPILL_PROBABILITY", "dataType": "Double"}, {"name": "FLT_NBR", "dataType": "Integer"}, {"name": "PYLD_PAX", "dataType": "Double"}, {"name": "REGION", "dataType": "String"}, {"name": "OPER_CARR_AIRLINE_CODE", "dataType": "String"}, {"name": "EQUIPMENT_TYPE", "dataType": "String"}, {"name": "REV_LF", "dataType": "Decimal"}, {"name": "FLEET", "dataType": "String"}, {"name": "CABIN", "dataType": "String"}, {"name": "LEG_OD", "dataType": "String"}, {"name": "CAP", "dataType": "Double"}, {"name": "SC_DPTR_DATE", "dataType": "Timestamp"}, {"name": "IATA_CODE", "dataType": "String"}]', 
        '[{"name": "SPILL_PROBABILITY", "dataType": "Double"}, {"name": "UNCONSTRAINED_LF", "dataType": "Double"}, {"name": "FLT_NBR", "dataType": "Integer"}, {"name": "DMD", "dataType": "Double"}, {"name": "PYLD_PAX", "dataType": "Double"}, {"name": "REFRESHED", "dataType": "Timestamp"}, {"name": "REGION", "dataType": "String"}, {"name": "OPER_CARR_AIRLINE_CODE", "dataType": "String"}, {"name": "IATA_EQUIP", "dataType": "String"}, {"name": "OBS_PAX", "dataType": "Double"}, {"name": "OBS_LF", "dataType": "Double"}, {"name": "EQUIPMENT_TYPE", "dataType": "String"}, {"name": "REV_LF", "dataType": "Decimal"}, {"name": "EQUIP", "dataType": "String"}, {"name": "FLEET", "dataType": "String"}, {"name": "CABIN", "dataType": "String"}, {"name": "LEG_OD", "dataType": "String"}, {"name": "CAP", "dataType": "Double"}, {"name": "SC_DPTR_DATE", "dataType": "Timestamp"}, {"name": "IATA_CODE", "dataType": "String"}, {"name": "K_FACTOR", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_17_0 AS (

  SELECT 
    (
      CASE
        WHEN ((REV_LF * (1 + SPILL_PROBABILITY)) < REV_LF)
          THEN CAST(REV_LF AS FLOAT64)
        ELSE (REV_LF * (1 + SPILL_PROBABILITY))
      END
    ) AS DMD_FACTOR,
    *
  
  FROM Union_95 AS in0

),

Formula_17_1 AS (

  SELECT 
    (
      CASE
        WHEN ((DMD_FACTOR * CAP) < PYLD_PAX)
          THEN PYLD_PAX
        ELSE (DMD_FACTOR * CAP)
      END
    ) AS UNCONSTRAINED_DMD,
    *
  
  FROM Formula_17_0 AS in0

),

Formula_17_2 AS (

  SELECT 
    (UNCONSTRAINED_DMD - PYLD_PAX) AS SPILL,
    *
  
  FROM Formula_17_1 AS in0

),

AlteryxSelect_23 AS (

  SELECT * EXCEPT (`FLEET`, `SPILL_PROBABILITY`, `DMD_FACTOR`)
  
  FROM Formula_17_2 AS in0

),

Join_70_right_UnionRightOuter AS (

  SELECT 
    in1.SC_DPTR_DATE AS SC_DPTR_DATE,
    in1.OPER_CARR_AIRLINE_CODE AS OPER_CARR_AIRLINE_CODE,
    in1.FLT_NBR AS FLT_NBR,
    in1.LEG_OD AS LEG_OD,
    in1.PYLD_PAX AS PYLD_PAX,
    in1.CAP AS CAP,
    in1.REV_LF AS REV_LF,
    in1.UNCONSTRAINED_DMD AS UNCONSTRAINED_DMD,
    in1.SPILL AS SPILL,
    in0.SPILL_FARE AS SPILL_FARE,
    in1.REGION AS REGION,
    in1.EQUIPMENT_TYPE AS EQUIPMENT_TYPE,
    in1.IATA_CODE AS IATA_CODE,
    in1.EQUIP AS EQUIP,
    in1.IATA_EQUIP AS IATA_EQUIP,
    in1.REFRESHED AS REFRESHED,
    (
      CASE
        WHEN (((in0.BASE_DPTR_DATE = in1.SC_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD)) AND (in0.CABIN = in1.CABIN))
          THEN in1.CABIN
        ELSE NULL
      END
    ) AS Right_CABIN,
    (
      CASE
        WHEN (((in0.BASE_DPTR_DATE = in1.SC_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD)) AND (in0.CABIN = in1.CABIN))
          THEN NULL
        ELSE in1.CABIN
      END
    ) AS CABIN,
    in0.* EXCEPT (`BASE_DPTR_DATE`, 
    `LEG_OD`, 
    `TTL_SPILL_REV`, 
    `SPILL_FARE`, 
    `TTL_BKGS`, 
    `TTL_ALTEA_SPILL`, 
    `TTL_EST_REV`, 
    `EST_FARE`, 
    `CABIN`),
    in1.* EXCEPT (`SC_DPTR_DATE`, 
    `OPER_CARR_AIRLINE_CODE`, 
    `FLT_NBR`, 
    `LEG_OD`, 
    `PYLD_PAX`, 
    `CAP`, 
    `REV_LF`, 
    `UNCONSTRAINED_DMD`, 
    `SPILL`, 
    `REGION`, 
    `EQUIPMENT_TYPE`, 
    `IATA_CODE`, 
    `EQUIP`, 
    `IATA_EQUIP`, 
    `REFRESHED`, 
    `CABIN`)
  
  FROM Filter_68 AS in0
  RIGHT JOIN AlteryxSelect_23 AS in1
     ON (((in0.BASE_DPTR_DATE = in1.SC_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD)) AND (in0.CABIN = in1.CABIN))

),

Cleanse_72 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_70_right_UnionRightOuter'], 
      [
        { "name": "UNCONSTRAINED_LF", "dataType": "Double" }, 
        { "name": "TTL_ALTEA_DMD", "dataType": "Double" }, 
        { "name": "FLT_NBR", "dataType": "Integer" }, 
        { "name": "Right_CABIN", "dataType": "String" }, 
        { "name": "DMD", "dataType": "Double" }, 
        { "name": "TTL_POSIT_ALTEA_SPILL", "dataType": "Double" }, 
        { "name": "PYLD_PAX", "dataType": "Double" }, 
        { "name": "REFRESHED", "dataType": "Timestamp" }, 
        { "name": "REGION", "dataType": "String" }, 
        { "name": "OPER_CARR_AIRLINE_CODE", "dataType": "String" }, 
        { "name": "UNCONSTRAINED_DMD", "dataType": "Double" }, 
        { "name": "SPILL_FARE", "dataType": "Double" }, 
        { "name": "IATA_EQUIP", "dataType": "String" }, 
        { "name": "OBS_PAX", "dataType": "Double" }, 
        { "name": "OBS_LF", "dataType": "Double" }, 
        { "name": "EQUIPMENT_TYPE", "dataType": "String" }, 
        { "name": "SPILL", "dataType": "Double" }, 
        { "name": "REV_LF", "dataType": "Decimal" }, 
        { "name": "EQUIP", "dataType": "String" }, 
        { "name": "CABIN", "dataType": "String" }, 
        { "name": "LEG_OD", "dataType": "String" }, 
        { "name": "CAP", "dataType": "Double" }, 
        { "name": "SC_DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "IATA_CODE", "dataType": "String" }, 
        { "name": "K_FACTOR", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'SC_DPTR_DATE', 
        'OPER_CARR_AIRLINE_CODE', 
        'FLT_NBR', 
        'LEG_OD', 
        'PYLD_PAX', 
        'CAP', 
        'REV_LF', 
        'UNCONSTRAINED_DMD', 
        'SPILL', 
        'SPILL_FARE', 
        'REGION', 
        'CABIN', 
        'EQUIPMENT_TYPE'
      ], 
      true, 
      '', 
      true, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Formula_19_0 AS (

  SELECT 
    (SPILL * SPILL_FARE) AS SPILL_REV,
    *
  
  FROM Cleanse_72 AS in0

),

Formula_19_1 AS (

  SELECT 
    (SPILL_REV / PYLD_PAX) AS SPILL_REV_PER_PAX,
    (SPILL_REV / SPILL) AS SPILL_REV_PER_SPILL,
    *
  
  FROM Formula_19_0 AS in0

),

AlteryxSelect_25 AS (

  SELECT 
    SC_DPTR_DATE AS DPTR_DT,
    FLT_NBR AS FLT_NBR,
    LEG_OD AS LEG_OD,
    REGION AS REGION,
    OPER_CARR_AIRLINE_CODE AS OPER_CARR_AIRLINE_CODE,
    EQUIPMENT_TYPE AS EQUIPMENT_TYPE,
    IATA_CODE AS IATA_CODE,
    CABIN AS CABIN,
    CAP AS CAP,
    PYLD_PAX AS PAX,
    REV_LF AS REV_LF,
    CAST(UNCONSTRAINED_DMD AS INT64) AS UNCONSTRAINED_DMD,
    CAST(SPILL AS INT64) AS SPILL,
    SPILL_FARE AS SPILL_FARE,
    SPILL_REV AS SPILL_REV,
    SPILL_REV_PER_PAX AS SPILL_REV_PER_PAX,
    SPILL_REV_PER_SPILL AS SPILL_REV_PER_SPILL
  
  FROM Formula_19_1 AS in0

)

SELECT *

FROM AlteryxSelect_25
