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

Filter_613 AS (

  SELECT * 
  
  FROM Formula_630_0 AS in0
  
  WHERE contains(FileName, 'NZ')

),

Formula_615_0 AS (

  SELECT 
    CAST({{ var('File_Browse_388') }} AS STRING) AS REPLACEMENT_PATH,
    *
  
  FROM Filter_613 AS in0

),

Formula_615_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (REPLACEMENT_PATH = 'PLACEHOLDER')
          THEN FullPath
        ELSE REPLACEMENT_PATH
      END
    ) AS STRING) AS FullPath,
    * EXCEPT (`fullpath`)
  
  FROM Formula_615_0 AS in0

)

SELECT *

FROM Formula_615_1
