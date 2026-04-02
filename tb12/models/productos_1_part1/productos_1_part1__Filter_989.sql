{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Join_988_inner AS (

  SELECT *
  
  FROM {{ ref('productos_1_part1__Join_988_inner')}}

),

Filter_989 AS (

  {#VisualGroup: CalculoProductosMDP#}
  SELECT * 
  
  FROM Join_988_inner AS in0
  
  WHERE (tipo_tarjeta = 'TC')

)

SELECT *

FROM Filter_989
