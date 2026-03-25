{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DataEncoderDecoder_1 AS (

  {{ sf_test.DataEncoderDecoder() }}

)

SELECT *

FROM DataEncoderDecoder_1
