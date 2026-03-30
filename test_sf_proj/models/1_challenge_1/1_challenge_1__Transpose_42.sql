{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_29_1 AS (

  SELECT *
  
  FROM {{ ref('1_challenge_1__Formula_29_1')}}

),

Transpose_42_schemaTransform_0 AS (

  SELECT * EXCLUDE ("STATE_FIPS_CODE", 
         "COUNTY_FIPS_CODE", 
         "PERCENTAGE BELOW POVERTY LEVEL", 
         "AMI", 
         "HOUSING UNITS", 
         "MEDIAN MONTHLY HOUSING COST- OWNER", 
         "MEDIAN MONTHLY HOUSING COST  - RENTER", 
         "PERCENT INCOME PAID AS RENT")
  
  FROM Formula_29_1 AS in0

),

Transpose_42 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_42_schemaTransform_0'], 
      ['GEOID', 'INCOME CATEGORY'], 
      [
        'TWO OR MORE RACES', 
        'WHITE ONLY', 
        'BLACK OR AFRICAN AMERICAN ONLY', 
        'AMERICAN INDIAN AND ALASKA NATIVE ONLY', 
        'ASIAN ONLY', 
        'NATIVE HAWAIIAN AND OTHER PACIFIC ISLANDER ONLY', 
        'SOME OTHER RACE ONLY'
      ], 
      'Name', 
      'Value', 
      [
        'INCOME CATEGORY', 
        'GEOID', 
        'TWO OR MORE RACES', 
        'WHITE ONLY', 
        'BLACK OR AFRICAN AMERICAN ONLY', 
        'AMERICAN INDIAN AND ALASKA NATIVE ONLY', 
        'ASIAN ONLY', 
        'NATIVE HAWAIIAN AND OTHER PACIFIC ISLANDER ONLY', 
        'SOME OTHER RACE ONLY'
      ], 
      true
    )
  }}

)

SELECT *

FROM Transpose_42
