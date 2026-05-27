{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_AlteryxSelect_994",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Filter_989 AS (

  SELECT *
  
  FROM {{ ref('productos_part1__Filter_989')}}

),

Formula_992_0 AS (

  SELECT 
    CAST(CASE
      WHEN ((codigo_producto = '77') AND coalesce(contains(lower(upper(descripcion_crm)), lower('SOY')), false))
        THEN 37
      WHEN (
        (codigo_producto = '77')
        AND NOT (coalesce(contains(lower(upper(descripcion_crm)), lower('SOY')), false))
      )
        THEN 36
      WHEN ((codigo_producto = '80') AND coalesce(contains(lower(upper(descripcion_crm)), lower('SOY')), false))
        THEN 39
      WHEN (
        (codigo_producto = '80')
        AND NOT (coalesce(contains(lower(upper(descripcion_crm)), lower('SOY')), false))
      )
        THEN 38
      ELSE 99
    END AS INT) AS CodProducto,
    *
  
  FROM Filter_989 AS in0

),

Filter_993 AS (

  SELECT * 
  
  FROM Formula_992_0 AS in0
  
  WHERE (
          NOT(
            CodProducto = 99)
        )

),

AlteryxSelect_994 AS (

  SELECT 
    idf_pers_ods_tarjeta AS idf_pers_ods,
    CodProducto AS CodProducto
  
  FROM Filter_993 AS in0

)

SELECT *

FROM AlteryxSelect_994
