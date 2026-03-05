{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH data_1_FL_csv_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('1_challenge_1', 'data_1_FL_csv_1') }}

),

Filter_17 AS (

  SELECT * 
  
  FROM data_1_FL_csv_1 AS in0
  
  WHERE (county = '095')

),

AlteryxSelect_3 AS (

  SELECT 
    geoid AS geoid,
    state_fips_code AS state_fips_code,
    county_fips_code AS county_fips_code,
    CAST(s1701_c03_001e AS FLOAT64) AS `Percentage Below Poverty Level`,
    CAST(s1903_c03_001e AS FLOAT64) AS AMI,
    CAST(pre1960 AS FLOAT64) AS `Housing units`,
    CAST(dp05_0035pe AS FLOAT64) AS `two or more races`,
    CAST(dp05_0037pe AS FLOAT64) AS `white only`,
    CAST(dp05_0038pe AS FLOAT64) AS `black or african american only`,
    CAST(dp05_0039pe AS FLOAT64) AS `american indian and alaska native only`,
    CAST(dp05_0044pe AS FLOAT64) AS `asian only`,
    CAST(dp05_0052pe AS FLOAT64) AS `native hawaiian and other pacific islander only`,
    CAST(dp05_0057pe AS FLOAT64) AS `some other race only`,
    CAST(s2503_c03_024e AS FLOAT64) AS `Median Monthly Housing Cost- Owner`,
    CAST(s2503_c05_024e AS FLOAT64) AS `Median Monthly Housing Cost  - Renter`
  
  FROM Filter_17 AS in0

),

Formula_29_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((AMI > 0) AND (AMI < 27750))
          THEN 'Extremly Low'
        WHEN ((AMI > 27750) AND (AMI <= 41450))
          THEN 'Very Low    '
        WHEN ((AMI > 41450) AND (AMI <= 66300))
          THEN 'Low         '
        WHEN ((AMI > 66300) AND (AMI <= 96120))
          THEN 'Moderate    '
        WHEN (AMI > 96120)
          THEN 'Upper       '
        ELSE 'None        '
      END
    ) AS STRING) AS `Income Category`,
    (
      CASE
        WHEN (`Median Monthly Housing Cost- Owner` < 0)
          THEN 0
        ELSE `Median Monthly Housing Cost- Owner`
      END
    ) AS `Median Monthly Housing Cost- Owner`,
    (
      CASE
        WHEN (`Median Monthly Housing Cost  - Renter` < 0)
          THEN 0
        ELSE `Median Monthly Housing Cost  - Renter`
      END
    ) AS `Median Monthly Housing Cost  - Renter`,
    * EXCEPT (`median monthly housing cost- owner`, `median monthly housing cost  - renter`)
  
  FROM AlteryxSelect_3 AS in0

),

Formula_29_1 AS (

  SELECT 
    ((`Median Monthly Housing Cost  - Renter` / AMI) * 100) AS `percent income paid as rent`,
    *
  
  FROM Formula_29_0 AS in0

)

SELECT *

FROM Formula_29_1
