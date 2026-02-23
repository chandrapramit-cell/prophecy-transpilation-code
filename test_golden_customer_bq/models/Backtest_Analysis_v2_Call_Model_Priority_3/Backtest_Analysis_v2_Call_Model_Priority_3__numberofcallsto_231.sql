{{
  config({    
    "materialized": "table",
    "alias": "numberofcallsto_231_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH Formula_93_1 AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Formula_93_1')}}

)

SELECT *

FROM Formula_93_1
