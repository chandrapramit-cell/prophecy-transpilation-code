{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH housingdata_csv_49 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('1_challenge_1', 'housingdata_csv_49') }}

),

Formula_50_0 AS (

  {#Generates geoid candidates by combining tract identifiers with sub-tracts for housing data.#}
  SELECT CAST((CONCAT('120950', Tract, `sub tract`)) AS STRING) AS Geoid
  
  FROM housingdata_csv_49 AS in0

),

AlteryxSelect_54 AS (

  select  CAST (`Total housing units` AS INT64) as `Total housing units` ,  CAST (`Total vacant houses` AS INT64) as `Total vacant houses` ,  CAST (`    For rent` AS INT64) as `    For rent` ,  CAST (`    Rentedcomma not occupied` AS INT64) as `    Rentedcomma not occupied` ,  CAST (`    For sale only` AS INT64) as `    For sale only` ,  CAST (`    Soldcomma not occupied` AS INT64) as `    Soldcomma not occupied` ,  CAST (`    For seasonalcomma recreationalcomma or occasional use` AS INT64) as `    For seasonalcomma recreationalcomma or occasional use` ,  CAST (`    For migrant workers` AS INT64) as `    For migrant workers` ,  CAST (`    Other vacant` AS INT64) as `    Other vacant` , *   REPLACE( Geoid as `Geoid` ) from Formula_50_0

),

Formula_29_1 AS (

  SELECT *
  
  FROM {{ ref('1_challenge_1__Formula_29_1')}}

),

Join_51_inner AS (

  SELECT 
    in0.* EXCEPT (`state_fips_code`, `county_fips_code`),
    in1.* EXCEPT (`Geoid`)
  
  FROM Formula_29_1 AS in0
  INNER JOIN AlteryxSelect_54 AS in1
     ON (in0.geoid = in1.Geoid)

),

Summarize_53 AS (

  SELECT 
    SUM(`Total vacant houses`) AS `Sum_Total vacant houses`,
    SUM(`    For rent`) AS `Sum_    For rent`,
    SUM(`    Rentedcomma not occupied`) AS `Sum_    Rentedcomma not occupied`,
    SUM(`    For sale only`) AS `Sum_    For sale only`,
    SUM(`    Soldcomma not occupied`) AS `Sum_    Soldcomma not occupied`,
    SUM(`    For migrant workers`) AS `Sum_    For migrant workers`,
    AVG(`Median Monthly Housing Cost  - Renter`) AS `Avg_Median Monthly Housing Cost  - Renter`,
    `Income Category` AS `Income Category`
  
  FROM Join_51_inner AS in0
  
  GROUP BY `Income Category`

),

Formula_56_0 AS (

  SELECT 
    ((`Sum_    For rent` / `Sum_Total vacant houses`) * 100) AS `Sum_    For rent`,
    ((`Sum_    Rentedcomma not occupied` / `Sum_Total vacant houses`) * 100) AS `Sum_    Rentedcomma not occupied`,
    ((`Sum_    For sale only` / `Sum_Total vacant houses`) * 100) AS `Sum_    For sale only`,
    ((`Sum_    Soldcomma not occupied` / `Sum_Total vacant houses`) * 100) AS `Sum_    Soldcomma not occupied`,
    ((`Sum_    For migrant workers` / `Sum_Total vacant houses`) * 100) AS `Sum_    For migrant workers`,
    * EXCEPT (`sum_    soldcomma not occupied`, 
    `sum_    for rent`, 
    `sum_    rentedcomma not occupied`, 
    `sum_    for migrant workers`, 
    `sum_    for sale only`)
  
  FROM Summarize_53 AS in0

)

SELECT *

FROM Formula_56_0
