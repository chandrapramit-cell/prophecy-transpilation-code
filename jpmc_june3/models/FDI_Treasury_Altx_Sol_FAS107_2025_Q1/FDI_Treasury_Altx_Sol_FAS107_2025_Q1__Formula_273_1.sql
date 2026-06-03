{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Filter_251 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Filter_251')}}

),

ICDW_DEPBAL_CC__269 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FDI_Treasury_Altx_Sol_FAS107_2025_Q1', 'ICDW_DEPBAL_CC__269') }}

),

Join_270_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Filter_251 AS in0
  LEFT JOIN ICDW_DEPBAL_CC__269 AS in1
     ON (in0.CCXREF_SAP_COST_CENTER = in1.COSTCENTER)

),

RecordID_275 AS (

  {{
    prophecy_basics.RecordID(
      ['Join_270_left_UnionLeftOuter'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Formula_273_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (CCXREF_SAP_COST_CENTER = COSTCENTER)
          OR (CAST(CD_CONVERSION_IDENTIFIER AS string) IN ('WMU', 'W40'))
        )
          THEN 'WAMU'
        ELSE 'CHASE'
      END
    ) AS string) AS Heritage,
    CAST(HCS_LOB_NODE04_NB AS string) AS HCS_LOB_NODE04_Orig,
    CAST((
      CASE
        WHEN CAST((CAST(HCS_LOB_NODE04_NB AS string) IN ('S570405', 'S578955', 'S577279', 'S576175')) AS BOOLEAN)
          THEN HCS_LOB_NODE04_NB
        WHEN (HCS_LOB_NODE03_NB = 'S577006')
          THEN HCS_LOB_NODE04_NB
        ELSE HCS_LOB_NODE03_NB
      END
    ) AS string) AS HCS_LOB_NODE04_NB,
    * EXCEPT (`hcs_lob_node04_nb`)
  
  FROM RecordID_275 AS in0

),

Formula_273_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (HCS_LOB_NODE04_NB = 'S576344')
          THEN 'CONSUMER & BUSINESS BANKING'
        WHEN (HCS_LOB_NODE04_NB = 'S536099')
          THEN 'CARD COMMERCE SOLUTIONS AND AUTO'
        WHEN (HCS_LOB_NODE04_NB = 'S535852')
          THEN 'GIM INCLUDING HBG- DD'
        WHEN (HCS_LOB_NODE04_NB = 'S551549')
          THEN 'COMMUNITY DEVELOPMENT- CI'
        WHEN (HCS_LOB_NODE04_NB = 'S556845')
          THEN 'COMMERCIAL TERM LENDING- CH'
        WHEN (HCS_LOB_NODE04_NB = 'S570325')
          THEN 'REAL ESTATE- CA'
        WHEN (HCS_LOB_NODE04_NB = 'S570405')
          THEN 'MIDDLE MARKET- CD'
        WHEN (HCS_LOB_NODE04_NB = 'S570512')
          THEN 'COMMERCIAL BANK ADMINISTRATION- CF'
        WHEN (HCS_LOB_NODE04_NB = 'S570545')
          THEN 'TO TRANSFER OUT OF COMML BANK- CG'
        WHEN (HCS_LOB_NODE04_NB = 'S572323')
          THEN 'CIB RISK- GS'
        WHEN (HCS_LOB_NODE04_NB = 'S572332')
          THEN 'GLOBAL COVERAGE & MGMT- GA'
        WHEN (HCS_LOB_NODE04_NB = 'S572391')
          THEN 'GLOBAL MANAGEMENT & OTHER NON BUSI- GF'
        WHEN (HCS_LOB_NODE04_NB = 'S576175')
          THEN 'TREASURY SERVICES- GR'
        WHEN (HCS_LOB_NODE04_NB = 'S576549')
          THEN 'CHIEF OPERATING OFFICE- T'
        WHEN (HCS_LOB_NODE04_NB = 'S577279')
          THEN 'CORPORATE CLIENT BANKING- CE'
        WHEN (HCS_LOB_NODE04_NB = 'S578955')
          THEN 'GLOBAL WEALTH MANAGEMENT- DB'
        WHEN (HCS_LOB_NODE04_NB = 'S579000')
          THEN 'CORPORATE ALIGNED- E'
        WHEN (HCS_LOB_NODE04_NB = 'S582269')
          THEN 'CIO - TREASURY SUMMARY- J'
        ELSE NULL
      END
    ) AS string) AS LOB_DESC,
    CAST(1 AS INTEGER) AS `Count`,
    *
  
  FROM Formula_273_0 AS in0

)

SELECT *

FROM Formula_273_1
