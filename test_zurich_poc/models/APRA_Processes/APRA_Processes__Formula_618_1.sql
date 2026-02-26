{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_630_0 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_630_0')}}

),

Filter_613_reject AS (

  SELECT * 
  
  FROM Formula_630_0 AS in0
  
  WHERE (NOT (contains(FileName, 'NZ')) OR isnull(contains(FileName, 'NZ')))

),

Formula_618_0 AS (

  SELECT 
    CAST({{ var('File_Browse_386') }} AS STRING) AS REPLACEMENT_PATH,
    *
  
  FROM Filter_613_reject AS in0

),

Formula_618_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (REPLACEMENT_PATH = 'PLACEHOLDER')
          THEN FullPath
        ELSE REPLACEMENT_PATH
      END
    ) AS STRING) AS FullPath,
    * EXCEPT (`fullpath`)
  
  FROM Formula_618_0 AS in0

)

SELECT *

FROM Formula_618_1
