{{
  config({    
    "materialized": "table",
    "alias": "Database__LOADI_111_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH Formula_7_0 AS (

  SELECT *
  
  FROM {{ ref('boeing__Formula_7_0')}}

),

Summarize_113 AS (

  SELECT 
    SUM(ALTEA_DMD) AS Sum_ALTEA_DMD,
    SUM(BKGS) AS Sum_BKGS,
    SUM(ALTEA_SPILL) AS Sum_ALTEA_SPILL,
    SUM(POSIT_ALTEA_SPILL) AS Sum_POSIT_ALTEA_SPILL,
    BASE_DPTR_DATE AS BASE_DPTR_DATE,
    LEG_OD AS LEG_OD,
    CABIN AS CABIN,
    CLASS_OF_SERVICE_CODE AS CLASS_OF_SERVICE_CODE
  
  FROM Formula_7_0 AS in0
  
  GROUP BY 
    BASE_DPTR_DATE, LEG_OD, CABIN, CLASS_OF_SERVICE_CODE

),

Unique_97 AS (

  SELECT *
  
  FROM {{ ref('boeing__Unique_97')}}

),

Summarize_120 AS (

  SELECT 
    SUM(SPILL) AS Sum_SPILL,
    DPTR_DT AS DPTR_DT,
    LEG_OD AS LEG_OD,
    CABIN AS CABIN,
    REGION AS REGION
  
  FROM Unique_97 AS in0
  
  GROUP BY 
    DPTR_DT, LEG_OD, CABIN, REGION

),

Filter_68 AS (

  SELECT *
  
  FROM {{ ref('boeing__Filter_68')}}

),

Join_126_inner AS (

  SELECT 
    in1.LEG_OD AS Right_LEG_OD,
    in1.CABIN AS Right_CABIN,
    in0.*,
    in1.* EXCEPT (`LEG_OD`, `CABIN`)
  
  FROM Summarize_113 AS in0
  INNER JOIN Summarize_120 AS in1
     ON (((in0.BASE_DPTR_DATE = in1.DPTR_DT) AND (in0.LEG_OD = in1.LEG_OD)) AND (in0.CABIN = in1.CABIN))

),

Join_133_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (
          ((in0.CABIN = in1.CABIN) AND (in0.LEG_OD = in1.LEG_OD))
          AND (in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE)
        )
          THEN in1.LEG_OD
        ELSE NULL
      END
    ) AS Right_LEG_OD,
    (
      CASE
        WHEN (
          ((in0.CABIN = in1.CABIN) AND (in0.LEG_OD = in1.LEG_OD))
          AND (in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE)
        )
          THEN in1.CABIN
        ELSE NULL
      END
    ) AS Right_CABIN,
    (
      CASE
        WHEN (
          ((in0.CABIN = in1.CABIN) AND (in0.LEG_OD = in1.LEG_OD))
          AND (in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE)
        )
          THEN in0.Right_LEG_OD
        ELSE NULL
      END
    ) AS Left_Right_LEG_OD,
    (
      CASE
        WHEN (
          ((in0.CABIN = in1.CABIN) AND (in0.LEG_OD = in1.LEG_OD))
          AND (in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE)
        )
          THEN in0.Right_CABIN
        ELSE NULL
      END
    ) AS Left_Right_CABIN,
    (
      CASE
        WHEN (
          ((in0.CABIN = in1.CABIN) AND (in0.LEG_OD = in1.LEG_OD))
          AND (in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE)
        )
          THEN in1.BASE_DPTR_DATE
        ELSE NULL
      END
    ) AS Right_BASE_DPTR_DATE,
    in0.* EXCEPT (`Right_LEG_OD`, `Right_CABIN`),
    in1.* EXCEPT (`CABIN`, `LEG_OD`, `BASE_DPTR_DATE`)
  
  FROM Join_126_inner AS in0
  LEFT JOIN Filter_68 AS in1
     ON (
      ((in0.CABIN = in1.CABIN) AND (in0.LEG_OD = in1.LEG_OD))
      AND (in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE)
    )

)

SELECT *

FROM Join_133_left_UnionLeftOuter
