{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_866 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Summarize_866')}}

),

Filter_867_reject AS (

  SELECT * 
  
  FROM Summarize_866 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_organization_name > 1)
          )
          OR ((CountDistinctNonNull_organization_name > 1) IS NULL)
        )

),

Join_863_inner AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Join_863_inner')}}

),

Join_869_inner AS (

  SELECT 
    in0.* EXCEPT (`EXP`, `CountDistinctNonNull_organization_name`),
    in1.* EXCEPT (`GroupID`)
  
  FROM Filter_867_reject AS in0
  INNER JOIN Join_863_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

Formula_870_0 AS (

  SELECT 
    CAST(organization_name AS string) AS new_organization_name_fuzzy_unique,
    *
  
  FROM Join_869_inner AS in0

),

Union_862_reformat_1 AS (

  SELECT 
    CAST(GroupID AS string) AS GroupID,
    city_town_village AS city_town_village,
    city_town_village_old AS city_town_village_old,
    lat AS lat,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    lot AS lot,
    new_organization_name_fuzzy_unique AS new_organization_name_fuzzy_unique,
    organization_name AS organization_name
  
  FROM Formula_870_0 AS in0

),

table_1004_Output6_macro_op AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_1004_Output6_macro_op') }}

),

Union_862_reformat_0 AS (

  SELECT 
    CAST(GroupID AS string) AS GroupID,
    city_town_village AS city_town_village,
    city_town_village_old AS city_town_village_old,
    lat AS lat,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    lot AS lot,
    new_organization_name_fuzzy_unique AS new_organization_name_fuzzy_unique,
    organization_name AS organization_name
  
  FROM table_1004_Output6_macro_op AS in0

),

Union_862 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_862_reformat_1', 'Union_862_reformat_0'], 
      [
        '[{"name": "new_organization_name_fuzzy_unique", "dataType": "String"}, {"name": "latitude", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "lot", "dataType": "Decimal"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Decimal"}]', 
        '[{"name": "new_organization_name_fuzzy_unique", "dataType": "String"}, {"name": "latitude", "dataType": "Decimal"}, {"name": "GroupID", "dataType": "Integer"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Decimal"}, {"name": "city_town_village", "dataType": "String"}, {"name": "lot", "dataType": "Decimal"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Decimal"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_961_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(new_organization_name_fuzzy_unique, '"', '')) AS string) AS new_organization_name_fuzzy_unique,
    * EXCEPT (`new_organization_name_fuzzy_unique`)
  
  FROM Union_862 AS in0

),

Filter_879 AS (

  SELECT * 
  
  FROM Formula_961_0 AS in0
  
  WHERE (NOT(longitude IS NULL))

),

Cleanse_886 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Filter_879'], 
      [
        { "name": "new_organization_name_fuzzy_unique", "dataType": "String" }, 
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "GroupID", "dataType": "String" }, 
        { "name": "organization_name", "dataType": "String" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "lot", "dataType": "Decimal" }, 
        { "name": "city_town_village_old", "dataType": "String" }, 
        { "name": "lat", "dataType": "Decimal" }
      ], 
      'keepOriginal', 
      ['organization_name'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Filter_887 AS (

  SELECT * 
  
  FROM Cleanse_886 AS in0
  
  WHERE ((LENGTH(new_organization_name_fuzzy_unique)) > 0)

),

Summarize_878 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN CAST((new_organization_name_fuzzy_unique IS NULL) AS BOOLEAN)
            THEN ''
          ELSE new_organization_name_fuzzy_unique
        END
      )) AS `Count`,
    new_organization_name_fuzzy_unique AS new_organization_name_fuzzy_unique,
    GroupID AS GroupID
  
  FROM Filter_887 AS in0
  
  GROUP BY 
    new_organization_name_fuzzy_unique, GroupID

),

RegEx_929 AS (

  {{
    prophecy_basics.Regex(
      ['Summarize_878'], 
      [], 
      '[{"name": "new_organization_name_fuzzy_unique", "dataType": "String"}, {"name": "GroupID", "dataType": "String"}, {"name": "Count", "dataType": "Double"}]', 
      'new_organization_name_fuzzy_unique', 
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

RegEx_929_typeCastGem AS (

  SELECT 
    CAST(starts_with_uppercase_letter AS BOOLEAN) AS starts_with_uppercase_letter,
    * EXCEPT (`starts_with_uppercase_letter`)
  
  FROM RegEx_929 AS in0

),

Filter_928 AS (

  SELECT * 
  
  FROM RegEx_929_typeCastGem AS in0
  
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
                                                                                          (
                                                                                            (length(new_organization_name_fuzzy_unique) < 3)
                                                                                            OR coalesce(contains(lower(new_organization_name_fuzzy_unique), lower('?')), false)
                                                                                          )
                                                                                          OR (substring(new_organization_name_fuzzy_unique, 1, 1) = '(')
                                                                                        )
                                                                                        OR (starts_with_uppercase_letter = true)
                                                                                      )
                                                                                      OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' STE ')), false)
                                                                                    )
                                                                                    OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVE.')), false)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' BLVD ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RUE ')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' BLVD.')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower('BOULEVARD')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVENUE')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ST.')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ST,')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' STR.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' UL.')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RD,')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' LANE')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PLAZA')), false)
                                                          )
                                                          OR (
                                                               substring(
                                                                 CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                                                 ((length(new_organization_name_fuzzy_unique) - 2) + 1), 
                                                                 2) = ' W'
                                                             )
                                                        )
                                                        OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PLAZA')), false)
                                                      )
                                                      OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ASSOCIATES')), false)
                                                    )
                                                    OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PARK')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RD.')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' QUAY')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' SQUARE DU')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVE ')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PARKWAY')), false)
                                        )
                                        OR (
                                             substring(
                                               CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                               ((length(new_organization_name_fuzzy_unique) - 6) + 1), 
                                               6) = 'STREET'
                                           )
                                      )
                                      OR (
                                           substring(
                                             CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                             ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                             4) = 'ROAD'
                                         )
                                    )
                                    OR (
                                         substring(
                                           CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                           ((length(new_organization_name_fuzzy_unique) - 6) + 1), 
                                           6) = 'AVENUE'
                                       )
                                  )
                                  OR (
                                       substring(
                                         CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                         ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                                         3) = 'BLV'
                                     )
                                )
                                OR (
                                     substring(
                                       CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                       ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                       4) = 'BLVD'
                                   )
                              )
                              OR (
                                   substring(
                                     CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                     ((length(new_organization_name_fuzzy_unique) - 5) + 1), 
                                     5) = 'PLAZA'
                                 )
                            )
                            OR (
                                 substring(
                                   CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                   ((length(new_organization_name_fuzzy_unique) - 5) + 1), 
                                   5) = 'DRIVE'
                               )
                          )
                          OR (
                               substring(
                                 CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                 ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                 4) = ' WAY'
                             )
                        )
                        OR (
                             substring(
                               CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                               ((length(new_organization_name_fuzzy_unique) - 8) + 1), 
                               8) = ' HIGHWAY'
                           )
                      )
                      OR (
                           substring(
                             CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                             ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                             3) = 'AVE'
                         )
                    )
                    OR (
                         substring(
                           CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                           ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                           3) = 'STR'
                       )
                  )
                  OR (
                       substring(
                         CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                         ((length(new_organization_name_fuzzy_unique) - 2) + 1), 
                         2) = 'DR'
                     )
                )
                OR (
                     substring(
                       CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                       ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                       3) = ' ST'
                   )
              )
              OR (
                   substring(
                     CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                     ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                     3) = ' RD'
                 )
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
                                                                                             (
                                                                                               (length(new_organization_name_fuzzy_unique) < 3)
                                                                                               OR coalesce(contains(lower(new_organization_name_fuzzy_unique), lower('?')), false)
                                                                                             )
                                                                                             OR (substring(new_organization_name_fuzzy_unique, 1, 1) = '(')
                                                                                           )
                                                                                           OR (starts_with_uppercase_letter = true)
                                                                                         )
                                                                                         OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' STE ')), false)
                                                                                       )
                                                                                       OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVE.')), false)
                                                                                     )
                                                                                     OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' BLVD ')), false)
                                                                                   )
                                                                                   OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RUE ')), false)
                                                                                 )
                                                                                 OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' BLVD.')), false)
                                                                               )
                                                                               OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower('BOULEVARD')), false)
                                                                             )
                                                                             OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVENUE')), false)
                                                                           )
                                                                           OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ST.')), false)
                                                                         )
                                                                         OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ST,')), false)
                                                                       )
                                                                       OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' STR.')), false)
                                                                     )
                                                                     OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' UL.')), false)
                                                                   )
                                                                   OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RD,')), false)
                                                                 )
                                                                 OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' LANE')), false)
                                                               )
                                                               OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PLAZA')), false)
                                                             )
                                                             OR (
                                                                  substring(
                                                                    CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                                                    ((length(new_organization_name_fuzzy_unique) - 2) + 1), 
                                                                    2) = ' W'
                                                                )
                                                           )
                                                           OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PLAZA')), false)
                                                         )
                                                         OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ASSOCIATES')), false)
                                                       )
                                                       OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PARK')), false)
                                                     )
                                                     OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RD.')), false)
                                                   )
                                                   OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' QUAY')), false)
                                                 )
                                                 OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' SQUARE DU')), false)
                                               )
                                               OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVE ')), false)
                                             )
                                             OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PARKWAY')), false)
                                           )
                                           OR (
                                                substring(
                                                  CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                                  ((length(new_organization_name_fuzzy_unique) - 6) + 1), 
                                                  6) = 'STREET'
                                              )
                                         )
                                         OR (
                                              substring(
                                                CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                                ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                                4) = 'ROAD'
                                            )
                                       )
                                       OR (
                                            substring(
                                              CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                              ((length(new_organization_name_fuzzy_unique) - 6) + 1), 
                                              6) = 'AVENUE'
                                          )
                                     )
                                     OR (
                                          substring(
                                            CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                            ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                                            3) = 'BLV'
                                        )
                                   )
                                   OR (
                                        substring(
                                          CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                          ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                          4) = 'BLVD'
                                      )
                                 )
                                 OR (
                                      substring(
                                        CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                        ((length(new_organization_name_fuzzy_unique) - 5) + 1), 
                                        5) = 'PLAZA'
                                    )
                               )
                               OR (
                                    substring(
                                      CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                      ((length(new_organization_name_fuzzy_unique) - 5) + 1), 
                                      5) = 'DRIVE'
                                  )
                             )
                             OR (
                                  substring(
                                    CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                    ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                    4) = ' WAY'
                                )
                           )
                           OR (
                                substring(
                                  CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                  ((length(new_organization_name_fuzzy_unique) - 8) + 1), 
                                  8) = ' HIGHWAY'
                              )
                         )
                         OR (
                              substring(
                                CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                                3) = 'AVE'
                            )
                       )
                       OR (
                            substring(
                              CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                              ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                              3) = 'STR'
                          )
                     )
                     OR (
                          substring(
                            CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                            ((length(new_organization_name_fuzzy_unique) - 2) + 1), 
                            2) = 'DR'
                        )
                   )
                   OR (
                        substring(
                          CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                          ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                          3) = ' ST'
                      )
                 )
                 OR (
                      substring(
                        CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                        ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                        3) = ' RD'
                    )
               ))
        )

),

Summarize_883 AS (

  SELECT 
    MAX(Count) AS Max_Count,
    GroupID AS GroupID
  
  FROM Filter_928 AS in0
  
  GROUP BY GroupID

),

Filter_888_reject AS (

  SELECT * 
  
  FROM Summarize_883 AS in0
  
  WHERE (
          (
            NOT(
              Max_Count = CAST('1' AS DOUBLE))
          ) OR ((Max_Count = CAST('1' AS DOUBLE)) IS NULL)
        )

),

Join_890_inner AS (

  SELECT 
    in1.new_organization_name_fuzzy_unique AS new_org_name_alteryx,
    in1.Count AS `Count`,
    in1.GroupID AS GroupID
  
  FROM Filter_888_reject AS in0
  INNER JOIN Summarize_878 AS in1
     ON ((in0.GroupID = in1.GroupID) AND (in0.Max_Count = in1.Count))

),

Summarize_917 AS (

  SELECT 
    COUNT(DISTINCT new_org_name_alteryx) AS CountDistinctNonNull_new_org_name_alteryx,
    GroupID AS GroupID
  
  FROM Join_890_inner AS in0
  
  GROUP BY GroupID

),

Filter_918_reject AS (

  SELECT * 
  
  FROM Summarize_917 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_new_org_name_alteryx > 1)
          )
          OR ((CountDistinctNonNull_new_org_name_alteryx > 1) IS NULL)
        )

),

Join_913_inner AS (

  SELECT 
    in0.GroupID AS GroupID,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.Count AS `Count`
  
  FROM Filter_918_reject AS in0
  INNER JOIN Join_890_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

Filter_918 AS (

  SELECT * 
  
  FROM Summarize_917 AS in0
  
  WHERE (CountDistinctNonNull_new_org_name_alteryx > 1)

),

Join_916_inner AS (

  SELECT 
    in0.GroupID AS GroupID,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.Count AS `Count`
  
  FROM Filter_918 AS in0
  INNER JOIN Join_890_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

RegEx_926 AS (

  {{
    prophecy_basics.Regex(
      ['Join_916_inner'], 
      [], 
      '[{"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Double"}]', 
      'new_org_name_alteryx', 
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

RegEx_926_typeCastGem AS (

  SELECT 
    CAST(starts_with_uppercase_letter AS BOOLEAN) AS starts_with_uppercase_letter,
    * EXCEPT (`starts_with_uppercase_letter`)
  
  FROM RegEx_926 AS in0

),

Filter_915 AS (

  SELECT * 
  
  FROM RegEx_926_typeCastGem AS in0
  
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
                                                                                      (
                                                                                        (length(new_org_name_alteryx) < 3)
                                                                                        OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                      )
                                                                                      OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                    )
                                                                                    OR (starts_with_uppercase_letter = true)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                          )
                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                        )
                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                      )
                                                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                    )
                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                        )
                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                      )
                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                    )
                                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                  )
                                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                )
                                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                              )
                              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                            )
                            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                          )
                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                        )
                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                      )
                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                    )
                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                  )
                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                )
                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
              )
              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
            )
            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
          )
          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
        )

),

Summarize_920 AS (

  SELECT 
    first(new_org_name_alteryx) AS `1_new_org_name_alteryx`,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Filter_915 AS in0
  
  GROUP BY 
    Count, GroupID

),

Filter_915_reject AS (

  SELECT * 
  
  FROM RegEx_926_typeCastGem AS in0
  
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
                                                                                          (
                                                                                            (length(new_org_name_alteryx) < 3)
                                                                                            OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                          )
                                                                                          OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                        )
                                                                                        OR (starts_with_uppercase_letter = true)
                                                                                      )
                                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                    )
                                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                          )
                                                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                        )
                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                      )
                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                    )
                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                        )
                                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                      )
                                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                    )
                                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                                  )
                                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                                )
                                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                              )
                              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                            )
                            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                          )
                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                        )
                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                      )
                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                    )
                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
                  )
                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
                )
                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
              )
              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
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
                                                                                             (
                                                                                               (length(new_org_name_alteryx) < 3)
                                                                                               OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                             )
                                                                                             OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                           )
                                                                                           OR (starts_with_uppercase_letter = true)
                                                                                         )
                                                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                       )
                                                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                                     )
                                                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                                   )
                                                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                                 )
                                                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                               )
                                                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                             )
                                                                             OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                           )
                                                                           OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                         )
                                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                       )
                                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                                     )
                                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                                   )
                                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                                 )
                                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                               )
                                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                             )
                                                             OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                           )
                                                           OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                         )
                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                       )
                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                                     )
                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                                   )
                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                                 )
                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                               )
                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                             )
                                             OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                           )
                                           OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                         )
                                         OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                       )
                                       OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                                     )
                                     OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                                   )
                                   OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                                 )
                                 OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                               )
                               OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                             )
                             OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                           )
                           OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                         )
                         OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                       )
                       OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
                     )
                     OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
                   )
                   OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
                 )
                 OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
               ))
        )

),

Summarize_919 AS (

  SELECT 
    first(new_org_name_alteryx) AS `2_new_org_name_alteryx`,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Filter_915_reject AS in0
  
  GROUP BY 
    Count, GroupID

),

Union_921 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_919', 'Summarize_920'], 
      [
        '[{"name": "Count", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "2_new_org_name_alteryx", "dataType": "String"}]', 
        '[{"name": "Count", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "1_new_org_name_alteryx", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

MultiRowFormula_924_window AS (

  SELECT 
    *,
    lead(`2_new_org_name_alteryx`, 1) OVER (PARTITION BY GroupID ORDER BY GroupID ASC NULLS FIRST) AS `2_new_org_name_alteryx_lead1`,
    lag(`2_new_org_name_alteryx`, 1) OVER (PARTITION BY GroupID ORDER BY GroupID ASC NULLS FIRST) AS `2_new_org_name_alteryx_lag1`
  
  FROM Union_921 AS in0

),

MultiRowFormula_924_0 AS (

  SELECT 
    CASE
      WHEN ((length(`2_new_org_name_alteryx`) = 0) AND (length(`2_new_org_name_alteryx_lead1`) = 0))
        THEN `2_new_org_name_alteryx_lag1`
      WHEN ((length(`2_new_org_name_alteryx`) = 0) AND (length(`2_new_org_name_alteryx_lag1`) = 0))
        THEN `2_new_org_name_alteryx_lead1`
      ELSE `2_new_org_name_alteryx`
    END AS `2_new_org_name_alteryx`,
    * EXCEPT (`2_new_org_name_alteryx_lead1`, `2_new_org_name_alteryx_lag1`, `2_new_org_name_alteryx`)
  
  FROM MultiRowFormula_924_window AS in0

),

MultiRowFormula_925_row_id_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_row_id,
    *
  
  FROM MultiRowFormula_924_0 AS in0

),

MultiRowFormula_925_0 AS (

  SELECT 
    lead(`1_new_org_name_alteryx`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id ASC NULLS FIRST) AS `1_new_org_name_alteryx_lead1`,
    lag(`1_new_org_name_alteryx`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id ASC NULLS FIRST) AS `1_new_org_name_alteryx_lag1`,
    *
  
  FROM MultiRowFormula_925_row_id_0 AS in0

),

MultiRowFormula_925_1 AS (

  SELECT 
    CASE
      WHEN ((length(`1_new_org_name_alteryx`) = 0) AND (length(`1_new_org_name_alteryx_lead1`) = 0))
        THEN `1_new_org_name_alteryx_lag1`
      WHEN ((length(`1_new_org_name_alteryx`) = 0) AND (length(`1_new_org_name_alteryx_lag1`) = 0))
        THEN `1_new_org_name_alteryx_lead1`
      ELSE `1_new_org_name_alteryx`
    END AS `1_new_org_name_alteryx`,
    * EXCEPT (`1_new_org_name_alteryx_lead1`, `1_new_org_name_alteryx_lag1`, `1_new_org_name_alteryx`)
  
  FROM MultiRowFormula_925_0 AS in0

),

MultiRowFormula_925_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_925_1 AS in0

),

Formula_922_0 AS (

  SELECT 
    CAST(CASE
      WHEN CAST(isnull(`2_new_org_name_alteryx`) AS BOOLEAN)
        THEN `1_new_org_name_alteryx`
      ELSE `2_new_org_name_alteryx`
    END AS STRING) AS new_org_name_alteryx,
    *
  
  FROM MultiRowFormula_925_row_id_drop_0 AS in0

),

Union_914 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_913_inner', 'Formula_922_0'], 
      [
        '[{"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Double"}]', 
        '[{"name": "1_new_org_name_alteryx", "dataType": "String"}, {"name": "2_new_org_name_alteryx", "dataType": "String"}, {"name": "GroupID", "dataType": "String"}, {"name": "Count", "dataType": "Double"}, {"name": "new_org_name_alteryx", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_892 AS (

  SELECT 
    first(new_org_name_alteryx) AS new_org_name_alteryx,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Union_914 AS in0
  
  GROUP BY 
    Count, GroupID

),

Filter_888 AS (

  SELECT * 
  
  FROM Summarize_883 AS in0
  
  WHERE (Max_Count = CAST('1' AS DOUBLE))

),

Join_884_inner AS (

  SELECT 
    in1.new_organization_name_fuzzy_unique AS new_org_name_alteryx,
    in1.Count AS `Count`,
    in1.GroupID AS GroupID
  
  FROM Filter_888 AS in0
  INNER JOIN Summarize_878 AS in1
     ON ((in0.GroupID = in1.GroupID) AND (in0.Max_Count = in1.Count))

),

Summarize_902 AS (

  SELECT 
    COUNT(DISTINCT new_org_name_alteryx) AS CountDistinctNonNull_new_org_name_alteryx,
    GroupID AS GroupID
  
  FROM Join_884_inner AS in0
  
  GROUP BY GroupID

),

Filter_903_reject AS (

  SELECT * 
  
  FROM Summarize_902 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_new_org_name_alteryx > 1)
          )
          OR ((CountDistinctNonNull_new_org_name_alteryx > 1) IS NULL)
        )

),

Join_898_inner AS (

  SELECT 
    in0.GroupID AS GroupID,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.Count AS `Count`
  
  FROM Filter_903_reject AS in0
  INNER JOIN Join_884_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

Filter_903 AS (

  SELECT * 
  
  FROM Summarize_902 AS in0
  
  WHERE (CountDistinctNonNull_new_org_name_alteryx > 1)

),

Join_901_inner AS (

  SELECT 
    in0.GroupID AS GroupID,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.Count AS `Count`
  
  FROM Filter_903 AS in0
  INNER JOIN Join_884_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

RegEx_911 AS (

  {{
    prophecy_basics.Regex(
      ['Join_901_inner'], 
      [], 
      '[{"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Double"}]', 
      'new_org_name_alteryx', 
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

RegEx_911_typeCastGem AS (

  SELECT 
    CAST(starts_with_uppercase_letter AS BOOLEAN) AS starts_with_uppercase_letter,
    * EXCEPT (`starts_with_uppercase_letter`)
  
  FROM RegEx_911 AS in0

),

Filter_900_reject AS (

  SELECT * 
  
  FROM RegEx_911_typeCastGem AS in0
  
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
                                                                                          (
                                                                                            (length(new_org_name_alteryx) < 3)
                                                                                            OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                          )
                                                                                          OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                        )
                                                                                        OR (starts_with_uppercase_letter = true)
                                                                                      )
                                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                    )
                                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                          )
                                                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                        )
                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                      )
                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                    )
                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                        )
                                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                      )
                                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                    )
                                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                                  )
                                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                                )
                                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                              )
                              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                            )
                            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                          )
                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                        )
                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                      )
                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                    )
                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
                  )
                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
                )
                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
              )
              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
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
                                                                                             (
                                                                                               (length(new_org_name_alteryx) < 3)
                                                                                               OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                             )
                                                                                             OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                           )
                                                                                           OR (starts_with_uppercase_letter = true)
                                                                                         )
                                                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                       )
                                                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                                     )
                                                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                                   )
                                                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                                 )
                                                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                               )
                                                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                             )
                                                                             OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                           )
                                                                           OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                         )
                                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                       )
                                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                                     )
                                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                                   )
                                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                                 )
                                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                               )
                                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                             )
                                                             OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                           )
                                                           OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                         )
                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                       )
                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                                     )
                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                                   )
                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                                 )
                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                               )
                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                             )
                                             OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                           )
                                           OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                         )
                                         OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                       )
                                       OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                                     )
                                     OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                                   )
                                   OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                                 )
                                 OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                               )
                               OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                             )
                             OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                           )
                           OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                         )
                         OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                       )
                       OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
                     )
                     OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
                   )
                   OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
                 )
                 OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
               ))
        )

),

Summarize_904 AS (

  SELECT 
    first(new_org_name_alteryx) AS `2_new_org_name_alteryx`,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Filter_900_reject AS in0
  
  GROUP BY 
    Count, GroupID

),

Filter_900 AS (

  SELECT * 
  
  FROM RegEx_911_typeCastGem AS in0
  
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
                                                                                      (
                                                                                        (length(new_org_name_alteryx) < 3)
                                                                                        OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                      )
                                                                                      OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                    )
                                                                                    OR (starts_with_uppercase_letter = true)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                          )
                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                        )
                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                      )
                                                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                    )
                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                        )
                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                      )
                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                    )
                                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                  )
                                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                )
                                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                              )
                              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                            )
                            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                          )
                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                        )
                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                      )
                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                    )
                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                  )
                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                )
                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
              )
              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
            )
            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
          )
          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
        )

),

Summarize_905 AS (

  SELECT 
    first(new_org_name_alteryx) AS `1_new_org_name_alteryx`,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Filter_900 AS in0
  
  GROUP BY 
    Count, GroupID

),

Union_906 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_904', 'Summarize_905'], 
      [
        '[{"name": "Count", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "2_new_org_name_alteryx", "dataType": "String"}]', 
        '[{"name": "Count", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "1_new_org_name_alteryx", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

MultiRowFormula_909_window AS (

  SELECT 
    *,
    lead(`2_new_org_name_alteryx`, 1) OVER (PARTITION BY GroupID ORDER BY GroupID ASC NULLS FIRST) AS `2_new_org_name_alteryx_lead1`,
    lag(`2_new_org_name_alteryx`, 1) OVER (PARTITION BY GroupID ORDER BY GroupID ASC NULLS FIRST) AS `2_new_org_name_alteryx_lag1`
  
  FROM Union_906 AS in0

),

MultiRowFormula_909_0 AS (

  SELECT 
    CASE
      WHEN ((length(`2_new_org_name_alteryx`) = 0) AND (length(`2_new_org_name_alteryx_lead1`) = 0))
        THEN `2_new_org_name_alteryx_lag1`
      WHEN ((length(`2_new_org_name_alteryx`) = 0) AND (length(`2_new_org_name_alteryx_lag1`) = 0))
        THEN `2_new_org_name_alteryx_lead1`
      ELSE `2_new_org_name_alteryx`
    END AS `2_new_org_name_alteryx`,
    * EXCEPT (`2_new_org_name_alteryx_lead1`, `2_new_org_name_alteryx_lag1`, `2_new_org_name_alteryx`)
  
  FROM MultiRowFormula_909_window AS in0

),

MultiRowFormula_910_row_id_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_row_id,
    *
  
  FROM MultiRowFormula_909_0 AS in0

),

MultiRowFormula_910_0 AS (

  SELECT 
    lead(`1_new_org_name_alteryx`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id ASC NULLS FIRST) AS `1_new_org_name_alteryx_lead1`,
    lag(`1_new_org_name_alteryx`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id ASC NULLS FIRST) AS `1_new_org_name_alteryx_lag1`,
    *
  
  FROM MultiRowFormula_910_row_id_0 AS in0

),

MultiRowFormula_910_1 AS (

  SELECT 
    CASE
      WHEN ((length(`1_new_org_name_alteryx`) = 0) AND (length(`1_new_org_name_alteryx_lead1`) = 0))
        THEN `1_new_org_name_alteryx_lag1`
      WHEN ((length(`1_new_org_name_alteryx`) = 0) AND (length(`1_new_org_name_alteryx_lag1`) = 0))
        THEN `1_new_org_name_alteryx_lead1`
      ELSE `1_new_org_name_alteryx`
    END AS `1_new_org_name_alteryx`,
    * EXCEPT (`1_new_org_name_alteryx_lead1`, `1_new_org_name_alteryx_lag1`, `1_new_org_name_alteryx`)
  
  FROM MultiRowFormula_910_0 AS in0

),

MultiRowFormula_910_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_910_1 AS in0

),

Formula_907_0 AS (

  SELECT 
    CAST(CASE
      WHEN CAST(isnull(`2_new_org_name_alteryx`) AS BOOLEAN)
        THEN `1_new_org_name_alteryx`
      ELSE `2_new_org_name_alteryx`
    END AS STRING) AS new_org_name_alteryx,
    *
  
  FROM MultiRowFormula_910_row_id_drop_0 AS in0

),

Union_899 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_898_inner', 'Formula_907_0'], 
      [
        '[{"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Double"}]', 
        '[{"name": "1_new_org_name_alteryx", "dataType": "String"}, {"name": "2_new_org_name_alteryx", "dataType": "String"}, {"name": "GroupID", "dataType": "String"}, {"name": "Count", "dataType": "Double"}, {"name": "new_org_name_alteryx", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_889 AS (

  SELECT 
    first(new_org_name_alteryx) AS new_org_name_alteryx,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Union_899 AS in0
  
  GROUP BY 
    Count, GroupID

),

Union_891 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_889', 'Summarize_892'], 
      [
        '[{"name": "Count", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}]', 
        '[{"name": "Count", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_880_inner AS (

  SELECT 
    in1.latitude AS latitude,
    in1.organization_name AS organization_name,
    in1.longitude AS longitude,
    in1.city_town_village AS city_town_village,
    in1.lot AS lot,
    in0.new_org_name_alteryx AS new_org_name_alteryx,
    in1.city_town_village_old AS city_town_village_old,
    in1.lat AS lat,
    in1.GroupID AS GroupID
  
  FROM Union_891 AS in0
  INNER JOIN Formula_961_0 AS in1
     ON ((in0.GroupID = in1.GroupID) AND (in0.new_org_name_alteryx = in1.new_organization_name_fuzzy_unique))

),

Join_880_right AS (

  SELECT in0.*
  
  FROM Formula_961_0 AS in0
  ANTI JOIN Union_891 AS in1
     ON ((in1.GroupID = in0.GroupID) AND (in1.new_org_name_alteryx = in0.new_organization_name_fuzzy_unique))

),

AlteryxSelect_882 AS (

  SELECT 
    lat AS lat,
    lot AS lot,
    GroupID AS GroupID,
    city_town_village_old AS city_town_village_old,
    latitude AS latitude,
    longitude AS longitude,
    organization_name AS organization_name,
    city_town_village AS city_town_village,
    new_organization_name_fuzzy_unique AS new_org_name_alteryx
  
  FROM Join_880_right AS in0

),

Union_881 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_880_inner', 'AlteryxSelect_882'], 
      [
        '[{"name": "latitude", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "lot", "dataType": "Decimal"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Decimal"}]', 
        '[{"name": "latitude", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "lot", "dataType": "Decimal"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Decimal"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_893 AS (

  SELECT 
    COUNT(DISTINCT new_org_name_alteryx) AS CountDistinctNonNull_new_org_name_alteryx,
    GroupID AS GroupID
  
  FROM Union_881 AS in0
  
  GROUP BY GroupID

),

Filter_894 AS (

  SELECT * 
  
  FROM Summarize_893 AS in0
  
  WHERE (CountDistinctNonNull_new_org_name_alteryx > 0)

),

Join_895_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`GroupID`)
  
  FROM Filter_894 AS in0
  INNER JOIN Union_881 AS in1
     ON (in0.GroupID = in1.GroupID)

),

Filter_894_reject AS (

  SELECT * 
  
  FROM Summarize_893 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_new_org_name_alteryx > 0)
          )
          OR ((CountDistinctNonNull_new_org_name_alteryx > 0) IS NULL)
        )

),

Join_896_inner AS (

  SELECT 
    in1.latitude AS latitude,
    in1.organization_name AS organization_name,
    in1.longitude AS longitude,
    in0.CountDistinctNonNull_new_org_name_alteryx AS CountDistinctNonNull_new_org_name_alteryx,
    in1.city_town_village AS city_town_village,
    in1.lot AS lot,
    in1.lat AS lat
  
  FROM Filter_894_reject AS in0
  INNER JOIN Union_881 AS in1
     ON (in0.GroupID = in1.GroupID)

),

Union_897 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_895_inner', 'Join_896_inner'], 
      [
        '[{"name": "latitude", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Double"}, {"name": "lot", "dataType": "Decimal"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Decimal"}]', 
        '[{"name": "latitude", "dataType": "Double"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Double"}, {"name": "lot", "dataType": "Decimal"}, {"name": "lat", "dataType": "Decimal"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_897
