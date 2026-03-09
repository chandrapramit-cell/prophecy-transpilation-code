{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH produkt_klima_t_4 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Will_it_Snow_Today', 'produkt_klima_t_4') }}

),

DateTime_8_0 AS (

  SELECT 
    (FORMAT_TIMESTAMP('yyyy-MM-dd', (PARSE_TIMESTAMP('%Y%m%d', MESS_DATUM)))) AS DateTime_Out,
    *
  
  FROM produkt_klima_t_4 AS in0

),

AlteryxSelect_5 AS (

  SELECT 
    DateTime_Out AS DateTime_Out,
    RSK AS `Niederschlag in mm`,
    CAST(RSKF AS FLOAT64) AS Niederschlagsform,
    TMK AS `Tagesmittel Temp`
  
  FROM DateTime_8_0 AS in0

),

Formula_9_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Niederschlagsform IN (7, 8))
          THEN '1'
        ELSE '0'
      END
    ) AS STRING) AS Schnee,
    *
  
  FROM AlteryxSelect_5 AS in0

),

DateTime_11_0 AS (

  {#Extracts and formats date components for each record, tagging month values for reporting.#}
  SELECT 
    (FORMAT_TIMESTAMP('dd-MM', CAST(DateTime_Out AS TIMESTAMP))) AS `Tag ampersand Monat`,
    *
  
  FROM Formula_9_0 AS in0

),

AlteryxSelect_15 AS (

  SELECT 
    CAST(Schnee AS FLOAT64) AS Schnee,
    * EXCEPT (`Schnee`)
  
  FROM DateTime_11_0 AS in0

)

SELECT *

FROM AlteryxSelect_15
