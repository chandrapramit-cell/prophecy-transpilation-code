{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_AlteryxSelect_997",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_996_0 AS (

  SELECT *
  
  FROM {{ ref('productos_part1__Formula_996_0')}}

),

AlteryxSelect_997 AS (

  {#VisualGroup: CalculoTDCodigoProducto15#}
  SELECT 
    idf_pers_ods_tarjeta AS idf_pers_ods,
    CodProducto AS CodProducto,
    Col1 AS Col1,
    Col2 AS Col2,
    Col3 AS Col3,
    Col4 AS Col4,
    Col5 AS Col5,
    Col6 AS Col6,
    Col7 AS Col7,
    Col8 AS Col8
  
  FROM Formula_996_0 AS in0

)

SELECT *

FROM AlteryxSelect_997
