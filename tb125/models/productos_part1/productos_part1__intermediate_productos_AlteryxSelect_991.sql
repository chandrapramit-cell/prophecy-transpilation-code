{{
  config({    
    "materialized": "table",
    "alias": "intermediate_productos_AlteryxSelect_991",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_990_0 AS (

  SELECT *
  
  FROM {{ ref('productos_part1__Formula_990_0')}}

),

AlteryxSelect_991 AS (

  {#VisualGroup: CalculoTCCodigoProducto1436373839#}
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
    Col8 AS Col8,
    Col9 AS Col9
  
  FROM Formula_990_0 AS in0

)

{#VisualGroup: Intermediate Target Nodes#}
SELECT *

FROM AlteryxSelect_991
