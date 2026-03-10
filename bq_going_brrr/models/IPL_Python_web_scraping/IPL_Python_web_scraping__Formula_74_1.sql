{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_63 AS (

  SELECT * 
  
  FROM {{ ref('seed_63')}}

),

TextInput_63_cast AS (

  SELECT 
    MatchID AS MatchID,
    `Match Number` AS `Match Number`,
    CAST(Team1 AS STRING) AS Team1,
    CAST(Team2 AS STRING) AS Team2
  
  FROM TextInput_63 AS in0

),

AlteryxSelect_75 AS (

  SELECT 
    CAST(`Match Number` AS STRING) AS `Match Number`,
    * EXCEPT (`Match Number`)
  
  FROM TextInput_63_cast AS in0

),

Formula_74_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (CAST((SUBSTRING(`Match Number`, (((LENGTH(`Match Number`)) - 1) + 1), 1)) AS STRING) = '1')
          THEN (CONCAT(`Match Number`, 'st'))
        WHEN (CAST((SUBSTRING(`Match Number`, (((LENGTH(`Match Number`)) - 1) + 1), 1)) AS STRING) = '2')
          THEN (CONCAT(`Match Number`, 'nd'))
        WHEN (CAST((SUBSTRING(`Match Number`, (((LENGTH(`Match Number`)) - 1) + 1), 1)) AS STRING) = '3')
          THEN (CONCAT(`Match Number`, 'rd'))
        ELSE (CONCAT(`Match Number`, 'th'))
      END
    ) AS STRING) AS `Match Number`,
    * EXCEPT (`match number`)
  
  FROM AlteryxSelect_75 AS in0

),

Formula_74_1 AS (

  SELECT 
    CAST((CONCAT('/', Team1, '-vs-', Team2, '-', `Match Number`, '-match-')) AS STRING) AS GameLink,
    *
  
  FROM Formula_74_0 AS in0

)

SELECT *

FROM Formula_74_1
