{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DateTimeNow_88_createRow AS (

  {{ sf_part5.create_data(n = 1, alias = 'seq') }}

),

DateTimeNow_88 AS (

  SELECT (TO_CHAR(CURRENT_TIMESTAMP, 'DD/MM/YY HH12:MI:SS')) AS DATETIMENOW
  
  FROM DateTimeNow_88_createRow AS in0

),

AlteryxSelect_42 AS (

  SELECT *
  
  FROM {{ ref('Cuadre_Fidessa__AlteryxSelect_42')}}

),

Formula_46_0 AS (

  SELECT *
  
  FROM {{ ref('Cuadre_Fidessa__Formula_46_0')}}

),

Join_48_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM AlteryxSelect_42 AS in0
  INNER JOIN Formula_46_0 AS in1
     ON (in0.STOCK2 = in1."ISIN NBR")

),

Formula_52_0 AS (

  SELECT 
    CAST(ABS(("QTY HYDRA NETO" - "QTY NTPA")) AS FLOAT) AS "QTY DIFERENCIA",
    CAST(ABS(("AMOUNT HYDRA NETO" - "AMOUNT NTPA")) AS FLOAT) AS "AMOUNT DIFERENCIA",
    *
  
  FROM Join_48_inner AS in0

),

AlteryxSelect_54 AS (

  SELECT * EXCLUDE ("ISIN NBR")
  
  FROM Formula_52_0 AS in0

),

AppendFields_89 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM DateTimeNow_88 AS in0
  INNER JOIN AlteryxSelect_54 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_89
