{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_29_1 AS (

  SELECT *
  
  FROM {{ ref('1_challenge_1__Formula_29_1')}}

),

Transpose_42_schemaTransform_0 AS (

  SELECT * EXCEPT (`state_fips_code`, 
         `county_fips_code`, 
         `Percentage Below Poverty Level`, 
         `AMI`, 
         `Housing units`, 
         `Median Monthly Housing Cost- Owner`, 
         `Median Monthly Housing Cost  - Renter`, 
         `percent income paid as rent`)
  
  FROM Formula_29_1 AS in0

),

Transpose_42 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_42_schemaTransform_0'], 
      ['geoid', 'Income Category'], 
      [
        'two or more races', 
        'white only', 
        'black or african american only', 
        'american indian and alaska native only', 
        'asian only', 
        'native hawaiian and other pacific islander only', 
        'some other race only'
      ], 
      'Name', 
      'Value', 
      [
        'Income Category', 
        'geoid', 
        'two or more races', 
        'white only', 
        'black or african american only', 
        'american indian and alaska native only', 
        'asian only', 
        'native hawaiian and other pacific islander only', 
        'some other race only'
      ], 
      true
    )
  }}

),

CrossTab_45 AS (

  SELECT *
  
  FROM (
    SELECT 
      geoid,
      `Income Category`,
      Name,
      VALUE
    
    FROM Transpose_42 AS in0
  )
  PIVOT (
    SUM(VALUE) AS Sum
    FOR Name
    IN (
      'white_only', 
      'american_indian_and_alaska_native_only', 
      'native_hawaiian_and_other_pacific_islander_only', 
      'some_other_race_only', 
      'two_or_more_races', 
      'black_or_african_american_only', 
      'asian_only'
    )
  )

),

CrossTab_45_rename AS (

  SELECT 
    Sum_white_only AS white_only,
    Sum_american_indian_and_alaska_native_only AS american_indian_and_alaska_native_only,
    Sum_native_hawaiian_and_other_pacific_islander_only AS native_hawaiian_and_other_pacific_islander_only,
    Sum_some_other_race_only AS some_other_race_only,
    Sum_two_or_more_races AS two_or_more_races,
    Sum_black_or_african_american_only AS black_or_african_american_only,
    Sum_asian_only AS asian_only,
    * EXCEPT (`Sum_white_only`, 
    `Sum_american_indian_and_alaska_native_only`, 
    `Sum_native_hawaiian_and_other_pacific_islander_only`, 
    `Sum_some_other_race_only`, 
    `Sum_two_or_more_races`, 
    `Sum_black_or_african_american_only`, 
    `Sum_asian_only`)
  
  FROM CrossTab_45 AS in0

)

SELECT *

FROM CrossTab_45_rename
