{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH DateTimeNow_10_createRow AS (

  {{ transpiler_data_matching.create_data(n = 1, alias = 'seq') }}

),

DateTimeNow_10 AS (

  SELECT (FORMAT_TIMESTAMP('yyyy-MM-dd', CURRENT_TIMESTAMP)) AS DateTimeNow
  
  FROM DateTimeNow_10_createRow AS in0

),

AlteryxSelect_13 AS (

  select *   REPLACE( (CASE WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DateTimeNow AS string)) IS NOT NULL) THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DateTimeNow AS string)) WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DateTimeNow AS string)) IS NOT NULL) THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DateTimeNow AS string)) ELSE SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(DateTimeNow AS string)) END) as `DateTimeNow` ) from DateTimeNow_10

),

DateTime_12_0 AS (

  SELECT 
    (FORMAT_TIMESTAMP('dd-MM', CAST(DateTimeNow AS DATETIME))) AS `Tag ampersand Monat Heute`,
    *
  
  FROM AlteryxSelect_13 AS in0

),

AlteryxSelect_15 AS (

  SELECT *
  
  FROM {{ ref('Will_it_Snow_Today__AlteryxSelect_15')}}

),

Summarize_14 AS (

  SELECT 
    APPROX_TOP_COUNT(`Tagesmittel Temp`, 1)[OFFSET(0)].value AS `Mode_Tagesmittel Temp`,
    SUM(Schnee) AS Sum_Schnee,
    COUNT(
      (
        CASE
          WHEN ((Schnee IS NULL) OR (CAST(Schnee AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Total Days`,
    `Tag ampersand Monat` AS `Tag ampersand Monat`
  
  FROM AlteryxSelect_15 AS in0
  
  GROUP BY `Tag ampersand Monat`

),

Formula_16_0 AS (

  SELECT 
    ((Sum_Schnee / `Total Days`) * 100) AS `Percentage Snow`,
    *
  
  FROM Summarize_14 AS in0

),

Sort_18 AS (

  SELECT * 
  
  FROM Formula_16_0 AS in0
  
  ORDER BY Sum_Schnee DESC

),

Join_26_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Sort_18 AS in0
  INNER JOIN DateTime_12_0 AS in1
     ON (in0.`Tag ampersand Monat` = in1.`Tag ampersand Monat Heute`)

),

AlteryxSelect_28 AS (

  SELECT 
    `Tag ampersand Monat` AS `Tag ampersand Monat`,
    Sum_Schnee AS Sum_Schnee,
    `Total Days` AS `Total Days`,
    `Percentage Snow` AS `Percentage Snow`
  
  FROM Join_26_inner AS in0

),

PortfolioComposerText_29 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

)

SELECT *

FROM PortfolioComposerText_29
