{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DateTimeNow_84_createRow AS (

  {{ sf_part5.create_data(n = 1, alias = 'seq') }}

),

DateTimeNow_84 AS (

  SELECT (TO_CHAR(CURRENT_TIMESTAMP, 'DD/MM/YY HH12:MI:SS')) AS DATETIMENOW
  
  FROM DateTimeNow_84_createRow AS in0

),

AlteryxSelect_42 AS (

  SELECT *
  
  FROM {{ ref('Cuadre_Fidessa__AlteryxSelect_42')}}

),

Formula_46_0 AS (

  SELECT *
  
  FROM {{ ref('Cuadre_Fidessa__Formula_46_0')}}

),

Join_48_right AS (

  SELECT in0.*
  
  FROM Formula_46_0 AS in0
  LEFT JOIN AlteryxSelect_42 AS in1
     ON (in1.STOCK2 = in0."ISIN NBR")

),

Cleanse_91 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_48_right'], 
      [
        { "name": "ISIN NBR", "dataType": "String" }, 
        { "name": "QTY NTPA", "dataType": "Double" }, 
        { "name": "AMOUNT NTPA", "dataType": "Double" }
      ], 
      'keepOriginal', 
      ['ISIN NBR', 'QTY NTPA', 'AMOUNT NTPA'], 
      true, 
      '', 
      false, 
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

AppendFields_85 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM DateTimeNow_84 AS in0
  INNER JOIN Cleanse_91 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_85
