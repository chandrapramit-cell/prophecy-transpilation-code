{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TextInput_57 AS (

  SELECT * 
  
  FROM {{ ref('seed_57')}}

),

TextInput_57_cast AS (

  SELECT 
    CAST(CountDistinctNonNull_new_org_name_alteryx AS INTEGER) AS CountDistinctNonNull_new_org_name_alteryx,
    CAST(GroupID AS INTEGER) AS GroupID,
    CAST(city_town_village AS string) AS city_town_village,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(organization_name AS string) AS organization_name,
    CAST(lat AS DOUBLE) AS lat,
    CAST(lot AS DOUBLE) AS lot,
    CAST(city_town_village_old AS string) AS city_town_village_old,
    CAST(new_org_name_alteryx AS string) AS new_org_name_alteryx
  
  FROM TextInput_57 AS in0

),

AlteryxSelect_40 AS (

  SELECT 
    GroupID AS variableGROUP,
    new_org_name_alteryx AS org_name_adj,
    * EXCEPT (`GroupID`, `new_org_name_alteryx`)
  
  FROM TextInput_57_cast AS in0

),

Filter_5 AS (

  SELECT * 
  
  FROM AlteryxSelect_40 AS in0
  
  WHERE (variableGROUP = CAST('2' AS INTEGER))

),

Unique_25 AS (

  SELECT * 
  
  FROM Filter_5 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY org_name_adj, variableGROUP ORDER BY org_name_adj, variableGROUP) = 1

),

RecordID_26 AS (

  {{
    prophecy_basics.RecordID(
      ['Unique_25'], 
      'incremental_id', 
      'RecordID', 
      'string', 
      1, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Formula_32_to_Formula_42_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(org_name_adj, '-', ' ')) AS string) AS org_name_adj,
    * EXCEPT (`org_name_adj`)
  
  FROM RecordID_26 AS in0

),

Formula_32_to_Formula_42_1 AS (

  SELECT 
    CAST((REGEXP_REPLACE(org_name_adj, '&', 'and')) AS string) AS org_name_adj,
    * EXCEPT (`org_name_adj`)
  
  FROM Formula_32_to_Formula_42_0 AS in0

),

Formula_32_to_Formula_42_2 AS (

  SELECT 
    CAST((REGEXP_REPLACE(org_name_adj, '1st', 'first')) AS string) AS org_name_adj,
    * EXCEPT (`org_name_adj`)
  
  FROM Formula_32_to_Formula_42_1 AS in0

),

Cleanse_27 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_32_to_Formula_42_2'], 
      [
        { "name": "org_name_adj", "dataType": "String" }, 
        { "name": "RecordID", "dataType": "String" }, 
        { "name": "variableGROUP", "dataType": "Integer" }, 
        { "name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Integer" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "organization_name", "dataType": "String" }, 
        { "name": "lat", "dataType": "Double" }, 
        { "name": "lot", "dataType": "Double" }, 
        { "name": "city_town_village_old", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['org_name_adj'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      false, 
      false, 
      true, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

TextToColumns_6 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Cleanse_27'], 
      'org_name_adj', 
      " ", 
      'splitRows', 
      1, 
      'leaveExtraCharLastCol', 
      'org_name_adj', 
      'org_name_adj', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_6_dropGem_0 AS (

  SELECT 
    generatedColumnName AS org_name_adj,
    * EXCEPT (`generatedColumnName`, `org_name_adj`)
  
  FROM TextToColumns_6 AS in0

),

Unique_37 AS (

  SELECT * 
  
  FROM TextToColumns_6_dropGem_0 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY RecordID, variableGROUP, org_name_adj ORDER BY RecordID, variableGROUP, org_name_adj) = 1

),

Cleanse_45 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Unique_37'], 
      [
        { "name": "org_name_adj", "dataType": "String" }, 
        { "name": "RecordID", "dataType": "String" }, 
        { "name": "variableGROUP", "dataType": "Integer" }, 
        { "name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Integer" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "organization_name", "dataType": "String" }, 
        { "name": "lat", "dataType": "Double" }, 
        { "name": "lot", "dataType": "Double" }, 
        { "name": "city_town_village_old", "dataType": "String" }
      ], 
      'makeTitleCase', 
      ['org_name_adj'], 
      true, 
      '', 
      true, 
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

Filter_48 AS (

  SELECT * 
  
  FROM Cleanse_45 AS in0
  
  WHERE (
          (
            NOT(
              CAST(org_name_adj AS string) IN (
                'Hospital',
                'Institute',
                'Clinic',
                'Department',
                'The',
                'University',
                'Centre',
                'Center',
                'Inc',
                'Llc',
                'Ltd',
                'Corp',
                'Corporation',
                'College'
              ))
          )
          OR (org_name_adj IS NULL)
        )

),

Summarize_7 AS (

  SELECT 
    concat_ws(',', collect_list(RecordID)) AS Concat_RecordID,
    COUNT(
      (
        CASE
          WHEN ((org_name_adj IS NULL) OR (CAST(org_name_adj AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    org_name_adj AS org_name_adj
  
  FROM Filter_48 AS in0
  
  GROUP BY org_name_adj

),

Filter_31 AS (

  SELECT * 
  
  FROM Summarize_7 AS in0
  
  WHERE contains(Concat_RecordID, ',')

),

MultiRowFormula_30_row_id_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_row_id,
    *
  
  FROM Filter_31 AS in0

),

MultiRowFormula_30_0 AS (

  SELECT 
    (LAG(Concat_RecordID, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS Concat_RecordID_lag1,
    (LEAD(Concat_RecordID, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS Concat_RecordID_lead1,
    *
  
  FROM MultiRowFormula_30_row_id_0 AS in0

),

MultiRowFormula_30_1 AS (

  SELECT 
    CASE
      WHEN contains(coalesce(lower(Concat_RecordID), ''), coalesce(lower(Concat_RecordID_lag1), ''))
        THEN Concat_RecordID_lag1
      WHEN contains(coalesce(lower(Concat_RecordID), ''), coalesce(lower(Concat_RecordID_lead1), ''))
        THEN Concat_RecordID_lead1
      ELSE Concat_RecordID
    END AS Concat_RecordID,
    * EXCEPT (`Concat_RecordID_lag1`, `Concat_RecordID_lead1`, `concat_recordid`)
  
  FROM MultiRowFormula_30_0 AS in0

),

MultiRowFormula_30_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_30_1 AS in0

),

Summarize_19 AS (

  SELECT 
    SUM(Count) AS Sum_Count,
    Concat_RecordID AS Concat_RecordID
  
  FROM MultiRowFormula_30_row_id_drop_0 AS in0
  
  GROUP BY Concat_RecordID

),

RecordID_23 AS (

  {{
    prophecy_basics.RecordID(
      ['Summarize_19'], 
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

Filter_24 AS (

  SELECT * 
  
  FROM RecordID_23 AS in0
  
  WHERE (RecordID = CAST('1' AS INTEGER))

),

Join_21_inner AS (

  SELECT 
    in1.Concat_RecordID AS Right_Concat_RecordID,
    in1.Sum_Count AS Right_Sum_Count,
    in0.*,
    in1.* EXCEPT (`Concat_RecordID`, `Sum_Count`)
  
  FROM Filter_24 AS in0
  INNER JOIN Summarize_19 AS in1
     ON (in0.Sum_Count = in1.Sum_Count)

),

Join_20_inner AS (

  SELECT 
    in0.Right_Concat_RecordID AS Left_Right_Concat_RecordID,
    in1.Concat_RecordID AS Right_Concat_RecordID,
    in0.* EXCEPT (`Right_Concat_RecordID`),
    in1.* EXCEPT (`Concat_RecordID`)
  
  FROM Join_21_inner AS in0
  INNER JOIN MultiRowFormula_30_row_id_drop_0 AS in1
     ON (in0.Concat_RecordID = in1.Concat_RecordID)

),

Summarize_8 AS (

  SELECT 
    concat_ws('', collect_list(org_name_adj)) AS org_name_adj,
    Concat_RecordID AS Concat_RecordID
  
  FROM Join_20_inner AS in0
  
  GROUP BY Concat_RecordID

),

AlteryxSelect_10 AS (

  SELECT org_name_adj AS org_name_adj
  
  FROM Filter_5 AS in0

),

Union_11 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_8', 'AlteryxSelect_10'], 
      [
        '[{"name": "org_name_adj", "dataType": "String"}, {"name": "Concat_RecordID", "dataType": "String"}]', 
        '[{"name": "org_name_adj", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

FuzzyMatch_9 AS (

  {{
    prophecy_basics.FuzzyMatch(
      ['Union_11'], 
      'PURGE', 
      'Account', 
      'org_name_adj', 
      [{ 'columnName': 'org_name_adj', 'matchFunction': 'custom' }], 
      60, 
      true
    )
  }}

),

FuzzyMatch_9_reformat AS (

  SELECT 
    RECORD_ID1 AS org_name_adj,
    RECORD_ID2 AS org_name_adj2,
    SIMILARITY_SCORE AS MatchScore,
    SIMILARITY_SCORE AS MatchScore_org_name_adj
  
  FROM FuzzyMatch_9 AS in0

),

Join_12_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`org_name_adj`)
  
  FROM FuzzyMatch_9_reformat AS in0
  INNER JOIN Summarize_8 AS in1
     ON (in0.org_name_adj = in1.org_name_adj)

),

Join_49_inner AS (

  SELECT 
    in0.org_name_adj AS org_name_adj2,
    in0.org_name_adj2 AS org_name_adj,
    in0.* EXCEPT (`org_name_adj`, `org_name_adj2`),
    in1.* EXCEPT (`org_name_adj`)
  
  FROM FuzzyMatch_9_reformat AS in0
  INNER JOIN Summarize_8 AS in1
     ON (in0.org_name_adj2 = in1.org_name_adj)

),

Union_44 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_12_inner', 'Join_49_inner'], 
      [
        '[{"name": "org_name_adj", "dataType": "String"}, {"name": "org_name_adj2", "dataType": "String"}, {"name": "MatchScore", "dataType": "Double"}, {"name": "MatchScore_org_name_adj", "dataType": "Double"}, {"name": "Concat_RecordID", "dataType": "String"}]', 
        '[{"name": "org_name_adj2", "dataType": "String"}, {"name": "org_name_adj", "dataType": "String"}, {"name": "MatchScore", "dataType": "Double"}, {"name": "MatchScore_org_name_adj", "dataType": "Double"}, {"name": "Concat_RecordID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Unique_17 AS (

  SELECT * 
  
  FROM Union_44 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY org_name_adj2, org_name_adj, MatchScore, MatchScore_org_name_adj ORDER BY org_name_adj2, org_name_adj, MatchScore, MatchScore_org_name_adj) = 1

),

RecordID_34 AS (

  {{
    prophecy_basics.RecordID(
      ['Unique_17'], 
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

AlteryxSelect_51 AS (

  SELECT org_name_adj AS org_name_keyword
  
  FROM Summarize_8 AS in0

),

Summarize_13 AS (

  SELECT 
    DISTINCT org_name_adj AS org_name_adj,
    MatchScore AS MatchScore
  
  FROM Union_44 AS in0

),

Summarize_14 AS (

  SELECT 
    MAX(MatchScore) AS Max_MatchScore,
    org_name_adj AS org_name_adj
  
  FROM Summarize_13 AS in0
  
  GROUP BY org_name_adj

),

Join_16_inner AS (

  SELECT 
    in0.Max_MatchScore AS Max_MatchScore,
    in1.org_name_adj2 AS max_org_name_adj,
    in1.org_name_adj AS org_name_adj
  
  FROM Summarize_14 AS in0
  INNER JOIN Union_44 AS in1
     ON ((in0.Max_MatchScore = in1.MatchScore) AND (in0.org_name_adj = in1.org_name_adj))

),

Unique_18 AS (

  SELECT * 
  
  FROM Join_16_inner AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Max_MatchScore, max_org_name_adj ORDER BY Max_MatchScore, max_org_name_adj) = 1

),

RecordID_47 AS (

  {{
    prophecy_basics.RecordID(
      ['Unique_18'], 
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

Filter_46 AS (

  SELECT * 
  
  FROM RecordID_47 AS in0
  
  WHERE (RecordID = CAST('1' AS INTEGER))

),

Filter_35 AS (

  SELECT * 
  
  FROM RecordID_34 AS in0
  
  WHERE (MatchScore_org_name_adj > 60)

),

Join_28_inner AS (

  SELECT 
    in0.max_org_name_adj AS max_org_name_adj,
    in1.org_name_adj2 AS org_name_adj2
  
  FROM Filter_46 AS in0
  INNER JOIN Filter_35 AS in1
     ON (in0.org_name_adj = in1.org_name_adj)

),

Unique_15 AS (

  SELECT * 
  
  FROM Join_28_inner AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY max_org_name_adj, org_name_adj2 ORDER BY max_org_name_adj, org_name_adj2) = 1

),

FindReplace_3_allRules AS (

  SELECT collect_list(struct(org_name_adj2 AS org_name_adj2, max_org_name_adj AS max_org_name_adj)) AS _rules
  
  FROM Unique_15 AS in0

),

FindReplace_3_join AS (

  SELECT 
    in0.latitude AS latitude,
    in0.org_name_adj AS org_name_adj,
    in0.organization_name AS organization_name,
    in0.longitude AS longitude,
    in0.CountDistinctNonNull_new_org_name_alteryx AS CountDistinctNonNull_new_org_name_alteryx,
    in0.city_town_village AS city_town_village,
    in1._rules AS _rules,
    in0.lot AS lot,
    in0.city_town_village_old AS city_town_village_old,
    in0.variableGROUP AS variableGROUP,
    in0.lat AS lat
  
  FROM Filter_5 AS in0
  FULL JOIN FindReplace_3_allRules AS in1
     ON TRUE

),

FindReplace_3_0 AS (

  SELECT 
    aggregate(
      _rules, 
      `org_name_adj`, 
      (acc, rule) -> regexp_replace(acc, concat('^', rule['org_name_adj2'], '$'), rule['max_org_name_adj'])) AS org_name_adj,
    * EXCEPT (`org_name_adj`)
  
  FROM FindReplace_3_join AS in0

),

FindReplace_3_reorg_0 AS (

  SELECT * EXCEPT (`_rules`)
  
  FROM FindReplace_3_0 AS in0

),

AppendFields_50 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM AlteryxSelect_51 AS in0
  INNER JOIN FindReplace_3_reorg_0 AS in1
     ON TRUE

),

AlteryxSelect_43 AS (

  SELECT 
    variableGROUP AS GroupID,
    org_name_adj AS new_org_name_alteryx,
    * EXCEPT (`variableGROUP`, `org_name_adj`)
  
  FROM AppendFields_50 AS in0

)

SELECT *

FROM AlteryxSelect_43
