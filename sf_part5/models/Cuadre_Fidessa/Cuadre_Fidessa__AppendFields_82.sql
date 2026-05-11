{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DateTimeNow_81_createRow AS (

  {{ sf_part5.create_data(n = 1, alias = 'seq') }}

),

AlteryxSelect_42 AS (

  SELECT *
  
  FROM {{ ref('Cuadre_Fidessa__AlteryxSelect_42')}}

),

Formula_46_0 AS (

  SELECT *
  
  FROM {{ ref('Cuadre_Fidessa__Formula_46_0')}}

),

Join_48_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_42 AS in0
  LEFT JOIN Formula_46_0 AS in1
     ON (in0.STOCK2 = in1."ISIN NBR")

),

DateTimeNow_81 AS (

  SELECT (TO_CHAR(CURRENT_TIMESTAMP, 'DD/MM/YY HH12:MI:SS')) AS DATETIMENOW
  
  FROM DateTimeNow_81_createRow AS in0

),

AppendFields_82 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Join_48_left AS in0
  INNER JOIN DateTimeNow_81 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_82
