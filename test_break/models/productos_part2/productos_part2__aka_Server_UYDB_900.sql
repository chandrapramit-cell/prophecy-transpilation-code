{{
  config({    
    "materialized": "table",
    "alias": "aka_Server_UYDB_900",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_895 AS (

  SELECT *
  
  FROM {{ ref('productos_part2__Union_895')}}

),

Filter_899 AS (

  SELECT * 
  
  FROM Union_895 AS in0
  
  WHERE (
          (NOT(IDF_PERS_ODS IS NULL))
          AND (
                (
                  NOT(
                    IDF_PERS_ODS = '9999999999999')
                ) OR (IDF_PERS_ODS IS NULL)
              )
        )

),

AlteryxSelect_896 AS (

  SELECT 
    IDF_PERS_ODS AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    CAST(Col2 AS string) AS Col2,
    CAST(Col3 AS string) AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6,
    Col7 AS Col7,
    Col8 AS Col8,
    Col9 AS Col9,
    Col10 AS Col10,
    Col11 AS Col11,
    Col12 AS Col12,
    Col13 AS Col13,
    Col14 AS Col14,
    Col15 AS Col15,
    Col16 AS Col16,
    Col17 AS Col17,
    Col18 AS Col18
  
  FROM Filter_899 AS in0

)

SELECT *

FROM AlteryxSelect_896
