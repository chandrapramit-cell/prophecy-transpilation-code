{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Filter_26_1004 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths_1004__Filter_26_1004')}}

),

AlteryxSelect_36_1004 AS (

  SELECT organization_name AS organization_name
  
  FROM Filter_26_1004 AS in0

),

Unique_35_1004 AS (

  SELECT * 
  
  FROM AlteryxSelect_36_1004 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY organization_name ORDER BY organization_name) = 1

),

RegEx_31_1004 AS (

  {{
    prophecy_basics.Regex(
      ['Unique_35_1004'], 
      [], 
      '[{"name": "organization_name", "dataType": "String"}]', 
      'organization_name', 
      '((?:^\w+)[A-Z_0-9](\s|:|,|-|l{1}|\+|/|\.).*$)|(^(\\u{1})(\s|-|\.|\+|,).*$)|((?:^\d).(-|\s|.|,|/+|(?:^\w+)).*$)|((?:^\w+)$)|((?:^\w+)[a-z_0-9](:|,|-|\+|/).*$)', 
      'match', 
      false, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      'starts_with_uppercase_letter', 
      false, 
      '_replaced'
    )
  }}

),

RegEx_31_1004_typeCastGem AS (

  SELECT 
    CAST(starts_with_uppercase_letter AS BOOLEAN) AS starts_with_uppercase_letter,
    * EXCEPT (`starts_with_uppercase_letter`)
  
  FROM RegEx_31_1004 AS in0

),

Filter_34_1004_reject AS (

  SELECT * 
  
  FROM RegEx_31_1004_typeCastGem AS in0
  
  WHERE (
          NOT (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          ((length(organization_name) < 3) OR coalesce(contains(lower(organization_name), lower('?')), false))
                                                                                          OR (starts_with_uppercase_letter = true)
                                                                                        )
                                                                                        OR (substring(organization_name, 1, 1) = '(')
                                                                                      )
                                                                                      OR coalesce(contains(lower(upper(organization_name)), lower(' STE ')), false)
                                                                                    )
                                                                                    OR coalesce(contains(lower(upper(organization_name)), lower(' AVE.')), false)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(organization_name)), lower(' BLVD ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(organization_name)), lower(' RUE ')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(organization_name)), lower(' BLVD.')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(organization_name)), lower('BOULEVARD')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(organization_name)), lower(' AVENUE')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(organization_name)), lower(' ST.')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(organization_name)), lower(' STR.')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(organization_name)), lower(' ST,')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(organization_name)), lower(' UL.')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(organization_name)), lower(' RD,')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(organization_name)), lower(' LANE')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(organization_name)), lower(' PLAZA')), false)
                                                          )
                                                          OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 2) + 1), 2) = ' W')
                                                        )
                                                        OR coalesce(contains(lower(upper(organization_name)), lower(' PLAZA')), false)
                                                      )
                                                      OR coalesce(contains(lower(upper(organization_name)), lower(' ASSOCIATES')), false)
                                                    )
                                                    OR coalesce(contains(lower(upper(organization_name)), lower(' PARK')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(organization_name)), lower(' RD.')), false)
                                                )
                                                OR coalesce(contains(lower(upper(organization_name)), lower(' QUAY')), false)
                                              )
                                              OR coalesce(contains(lower(upper(organization_name)), lower(' SQUARE DU')), false)
                                            )
                                            OR coalesce(contains(lower(upper(organization_name)), lower(' AVE ')), false)
                                          )
                                          OR coalesce(contains(lower(upper(organization_name)), lower(' PARKWAY')), false)
                                        )
                                        OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 6) + 1), 6) = 'STREET')
                                      )
                                      OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 4) + 1), 4) = 'ROAD')
                                    )
                                    OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 6) + 1), 6) = 'AVENUE')
                                  )
                                  OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = 'BLV')
                                )
                                OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 4) + 1), 4) = 'BLVD')
                              )
                              OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 5) + 1), 5) = 'PLAZA')
                            )
                            OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 5) + 1), 5) = 'DRIVE')
                          )
                          OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 4) + 1), 4) = ' WAY')
                        )
                        OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 8) + 1), 8) = ' HIGHWAY')
                      )
                      OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = 'AVE')
                    )
                    OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = 'STR')
                  )
                  OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 2) + 1), 2) = 'DR')
                )
                OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = ' ST')
              )
              OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = ' RD')
            )
          )
          OR isnull(
               (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     (
                                                       (
                                                         (
                                                           (
                                                             (
                                                               (
                                                                 (
                                                                   (
                                                                     (
                                                                       (
                                                                         (
                                                                           (
                                                                             (
                                                                               (
                                                                                 (
                                                                                   (
                                                                                     (
                                                                                       (
                                                                                         (
                                                                                           (
                                                                                             ((length(organization_name) < 3) OR coalesce(contains(lower(organization_name), lower('?')), false))
                                                                                             OR (starts_with_uppercase_letter = true)
                                                                                           )
                                                                                           OR (substring(organization_name, 1, 1) = '(')
                                                                                         )
                                                                                         OR coalesce(contains(lower(upper(organization_name)), lower(' STE ')), false)
                                                                                       )
                                                                                       OR coalesce(contains(lower(upper(organization_name)), lower(' AVE.')), false)
                                                                                     )
                                                                                     OR coalesce(contains(lower(upper(organization_name)), lower(' BLVD ')), false)
                                                                                   )
                                                                                   OR coalesce(contains(lower(upper(organization_name)), lower(' RUE ')), false)
                                                                                 )
                                                                                 OR coalesce(contains(lower(upper(organization_name)), lower(' BLVD.')), false)
                                                                               )
                                                                               OR coalesce(contains(lower(upper(organization_name)), lower('BOULEVARD')), false)
                                                                             )
                                                                             OR coalesce(contains(lower(upper(organization_name)), lower(' AVENUE')), false)
                                                                           )
                                                                           OR coalesce(contains(lower(upper(organization_name)), lower(' ST.')), false)
                                                                         )
                                                                         OR coalesce(contains(lower(upper(organization_name)), lower(' STR.')), false)
                                                                       )
                                                                       OR coalesce(contains(lower(upper(organization_name)), lower(' ST,')), false)
                                                                     )
                                                                     OR coalesce(contains(lower(upper(organization_name)), lower(' UL.')), false)
                                                                   )
                                                                   OR coalesce(contains(lower(upper(organization_name)), lower(' RD,')), false)
                                                                 )
                                                                 OR coalesce(contains(lower(upper(organization_name)), lower(' LANE')), false)
                                                               )
                                                               OR coalesce(contains(lower(upper(organization_name)), lower(' PLAZA')), false)
                                                             )
                                                             OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 2) + 1), 2) = ' W')
                                                           )
                                                           OR coalesce(contains(lower(upper(organization_name)), lower(' PLAZA')), false)
                                                         )
                                                         OR coalesce(contains(lower(upper(organization_name)), lower(' ASSOCIATES')), false)
                                                       )
                                                       OR coalesce(contains(lower(upper(organization_name)), lower(' PARK')), false)
                                                     )
                                                     OR coalesce(contains(lower(upper(organization_name)), lower(' RD.')), false)
                                                   )
                                                   OR coalesce(contains(lower(upper(organization_name)), lower(' QUAY')), false)
                                                 )
                                                 OR coalesce(contains(lower(upper(organization_name)), lower(' SQUARE DU')), false)
                                               )
                                               OR coalesce(contains(lower(upper(organization_name)), lower(' AVE ')), false)
                                             )
                                             OR coalesce(contains(lower(upper(organization_name)), lower(' PARKWAY')), false)
                                           )
                                           OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 6) + 1), 6) = 'STREET')
                                         )
                                         OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 4) + 1), 4) = 'ROAD')
                                       )
                                       OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 6) + 1), 6) = 'AVENUE')
                                     )
                                     OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = 'BLV')
                                   )
                                   OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 4) + 1), 4) = 'BLVD')
                                 )
                                 OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 5) + 1), 5) = 'PLAZA')
                               )
                               OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 5) + 1), 5) = 'DRIVE')
                             )
                             OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 4) + 1), 4) = ' WAY')
                           )
                           OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 8) + 1), 8) = ' HIGHWAY')
                         )
                         OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = 'AVE')
                       )
                       OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = 'STR')
                     )
                     OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 2) + 1), 2) = 'DR')
                   )
                   OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = ' ST')
                 )
                 OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = ' RD')
               ))
        )

),

CountRecords_43_1004 AS (

  SELECT COUNT('1') AS `Count`
  
  FROM Filter_34_1004_reject AS in0

),

Filter_42_1004 AS (

  SELECT * 
  
  FROM CountRecords_43_1004 AS in0
  
  WHERE (Count = CAST('0' AS INTEGER))

),

AlteryxSelect_46_1004 AS (

  SELECT 
    CAST(Count AS string) AS `Count`,
    * EXCEPT (`Count`)
  
  FROM Filter_42_1004 AS in0

),

Filter_42_1004_reject AS (

  SELECT * 
  
  FROM CountRecords_43_1004 AS in0
  
  WHERE (NOT ((Count = CAST('0' AS INT))) OR isnull((Count = CAST('0' AS INT))))

),

Formula_48_1004_0 AS (

  SELECT 
    CAST('countgt0' AS string) AS joinkeyforcountgt0,
    *
  
  FROM Filter_42_1004_reject AS in0

),

Filter_34_1004 AS (

  SELECT * 
  
  FROM RegEx_31_1004_typeCastGem AS in0
  
  WHERE (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      ((length(organization_name) < 3) OR coalesce(contains(lower(organization_name), lower('?')), false))
                                                                                      OR (starts_with_uppercase_letter = true)
                                                                                    )
                                                                                    OR (substring(organization_name, 1, 1) = '(')
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(organization_name)), lower(' STE ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(organization_name)), lower(' AVE.')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(organization_name)), lower(' BLVD ')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(organization_name)), lower(' RUE ')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(organization_name)), lower(' BLVD.')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(organization_name)), lower('BOULEVARD')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(organization_name)), lower(' AVENUE')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(organization_name)), lower(' ST.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(organization_name)), lower(' STR.')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(organization_name)), lower(' ST,')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(organization_name)), lower(' UL.')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(organization_name)), lower(' RD,')), false)
                                                          )
                                                          OR coalesce(contains(lower(upper(organization_name)), lower(' LANE')), false)
                                                        )
                                                        OR coalesce(contains(lower(upper(organization_name)), lower(' PLAZA')), false)
                                                      )
                                                      OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 2) + 1), 2) = ' W')
                                                    )
                                                    OR coalesce(contains(lower(upper(organization_name)), lower(' PLAZA')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(organization_name)), lower(' ASSOCIATES')), false)
                                                )
                                                OR coalesce(contains(lower(upper(organization_name)), lower(' PARK')), false)
                                              )
                                              OR coalesce(contains(lower(upper(organization_name)), lower(' RD.')), false)
                                            )
                                            OR coalesce(contains(lower(upper(organization_name)), lower(' QUAY')), false)
                                          )
                                          OR coalesce(contains(lower(upper(organization_name)), lower(' SQUARE DU')), false)
                                        )
                                        OR coalesce(contains(lower(upper(organization_name)), lower(' AVE ')), false)
                                      )
                                      OR coalesce(contains(lower(upper(organization_name)), lower(' PARKWAY')), false)
                                    )
                                    OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 6) + 1), 6) = 'STREET')
                                  )
                                  OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 4) + 1), 4) = 'ROAD')
                                )
                                OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 6) + 1), 6) = 'AVENUE')
                              )
                              OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = 'BLV')
                            )
                            OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 4) + 1), 4) = 'BLVD')
                          )
                          OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 5) + 1), 5) = 'PLAZA')
                        )
                        OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 5) + 1), 5) = 'DRIVE')
                      )
                      OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 4) + 1), 4) = ' WAY')
                    )
                    OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 8) + 1), 8) = ' HIGHWAY')
                  )
                  OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = 'AVE')
                )
                OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = 'STR')
              )
              OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 2) + 1), 2) = 'DR')
            )
            OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = ' ST')
          )
          OR (substring(CAST(upper(organization_name) AS STRING), ((length(organization_name) - 3) + 1), 3) = ' RD')
        )

),

Formula_45_1004_0 AS (

  SELECT 
    CAST(0 AS string) AS `Count`,
    *
  
  FROM Filter_34_1004 AS in0

),

Formula_49_1004_0 AS (

  SELECT 
    CAST('countgt0' AS string) AS joinkeyforcountgt0,
    *
  
  FROM Filter_34_1004_reject AS in0

),

Join_50_1004_inner AS (

  SELECT in1.organization_name AS organization_name
  
  FROM Formula_48_1004_0 AS in0
  INNER JOIN Formula_49_1004_0 AS in1
     ON (in0.joinkeyforcountgt0 = in1.joinkeyforcountgt0)

),

Join_44_1004_inner AS (

  SELECT in0.organization_name AS organization_name
  
  FROM Formula_45_1004_0 AS in0
  INNER JOIN AlteryxSelect_46_1004 AS in1
     ON (in0.Count = in1.Count)

),

Union_47_1004 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_44_1004_inner', 'Join_50_1004_inner'], 
      [
        '[{"name": "organization_name", "dataType": "String"}]', 
        '[{"name": "organization_name", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

FuzzyMatch_41_1004 AS (

  {{
    prophecy_basics.FuzzyMatch(
      ['Union_47_1004'], 
      'PURGE', 
      'Account', 
      'organization_name', 
      [{ 'columnName': 'organization_name', 'matchFunction': 'custom' }], 
      85, 
      true
    )
  }}

),

FuzzyMatch_41_1004_reformat AS (

  SELECT 
    RECORD_ID1 AS organization_name,
    RECORD_ID2 AS organization_name2,
    SIMILARITY_SCORE AS MatchScore,
    SIMILARITY_SCORE AS MatchScore_organization_name
  
  FROM FuzzyMatch_41_1004 AS in0

)

SELECT *

FROM FuzzyMatch_41_1004_reformat
