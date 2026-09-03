{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_2787 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2787')}}

),

Formula_2791_to_Formula_2794_0 AS (

  SELECT 
    (TO_DATE(First_PosRevMonth, 'yyyy-MM-dd')) AS Cohort,
    *
  
  FROM Formula_2787 AS in0

),

Formula_2791_to_Formula_2794_1 AS (

  SELECT 
    CAST(CAST((MONTHS_BETWEEN((TO_DATE(RevMonth)), (TO_DATE(Cohort)))) AS INTEGER) AS INTEGER) AS `Cohort Tenure`,
    CAST(`Customer Active Flag` AS INTEGER) AS `Cohort Active Flag`,
    (TO_DATE(Cohort, 'yyyy-MM-dd')) AS `Customer Level Cohort`,
    *
  
  FROM Formula_2791_to_Formula_2794_0 AS in0

),

Formula_2791_to_Formula_2794_2 AS (

  SELECT 
    CAST(`Cohort Tenure` AS INTEGER) AS `Customer Level Cohort Tenure`,
    CAST((ARRAY_MIN((ARRAY(`Initial Revenue`, Revenue)))) AS DOUBLE) AS `Cohort Gross Retention`,
    CAST((
      CASE
        WHEN (`Cohort Active Flag` = 1)
          THEN `Initial Revenue`
        ELSE 0
      END
    ) AS DOUBLE) AS `Cohort Gross Customer Retention`,
    *
  
  FROM Formula_2791_to_Formula_2794_1 AS in0

),

Filter_2795_reject AS (

  SELECT * 
  
  FROM Formula_2791_to_Formula_2794_2 AS in0
  
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
                                                      NOT(
                                                        `Customer Segment` = `Prev_Customer Segment`)
                                                    ) OR isnull(`Customer Segment`)
                                                  )
                                                  OR isnull(`Prev_Customer Segment`)
                                                )
                                                OR (
                                                     NOT(
                                                       SubCustSeg1 = Prev_SubCustSeg1)
                                                   )
                                              )
                                              OR isnull(SubCustSeg1)
                                            )
                                            OR isnull(Prev_SubCustSeg1)
                                          )
                                          OR (
                                               NOT(
                                                 SubCustSeg2 = Prev_SubCustSeg2)
                                             )
                                        )
                                        OR isnull(SubCustSeg2)
                                      )
                                      OR isnull(Prev_SubCustSeg2)
                                    )
                                    OR (
                                         NOT(
                                           SubCustSeg3 = Prev_SubCustSeg3)
                                       )
                                  )
                                  OR isnull(SubCustSeg3)
                                )
                                OR isnull(Prev_SubCustSeg3)
                              )
                              OR (
                                   NOT(
                                     SubCustSeg4 = Prev_SubCustSeg4)
                                 )
                            )
                            OR isnull(SubCustSeg4)
                          )
                          OR isnull(Prev_SubCustSeg4)
                        )
                        OR (
                             NOT(
                               SubCustSeg5 = Prev_SubCustSeg5)
                           )
                      )
                      OR isnull(SubCustSeg5)
                    )
                    OR isnull(Prev_SubCustSeg5)
                  )
                  OR (
                       NOT(
                         SubCustSeg6 = Prev_SubCustSeg6)
                     )
                )
                OR isnull(SubCustSeg6)
              )
              OR isnull(Prev_SubCustSeg6)
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
                                                         NOT(
                                                           `Customer Segment` = `Prev_Customer Segment`)
                                                       ) OR isnull(`Customer Segment`)
                                                     )
                                                     OR isnull(`Prev_Customer Segment`)
                                                   )
                                                   OR (
                                                        NOT(
                                                          SubCustSeg1 = Prev_SubCustSeg1)
                                                      )
                                                 )
                                                 OR isnull(SubCustSeg1)
                                               )
                                               OR isnull(Prev_SubCustSeg1)
                                             )
                                             OR (
                                                  NOT(
                                                    SubCustSeg2 = Prev_SubCustSeg2)
                                                )
                                           )
                                           OR isnull(SubCustSeg2)
                                         )
                                         OR isnull(Prev_SubCustSeg2)
                                       )
                                       OR (
                                            NOT(
                                              SubCustSeg3 = Prev_SubCustSeg3)
                                          )
                                     )
                                     OR isnull(SubCustSeg3)
                                   )
                                   OR isnull(Prev_SubCustSeg3)
                                 )
                                 OR (
                                      NOT(
                                        SubCustSeg4 = Prev_SubCustSeg4)
                                    )
                               )
                               OR isnull(SubCustSeg4)
                             )
                             OR isnull(Prev_SubCustSeg4)
                           )
                           OR (
                                NOT(
                                  SubCustSeg5 = Prev_SubCustSeg5)
                              )
                         )
                         OR isnull(SubCustSeg5)
                       )
                       OR isnull(Prev_SubCustSeg5)
                     )
                     OR (
                          NOT(
                            SubCustSeg6 = Prev_SubCustSeg6)
                        )
                   )
                   OR isnull(SubCustSeg6)
                 )
                 OR isnull(Prev_SubCustSeg6)
               ))
        )

),

Filter_2795 AS (

  SELECT * 
  
  FROM Formula_2791_to_Formula_2794_2 AS in0
  
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
                                                  NOT(
                                                    `Customer Segment` = `Prev_Customer Segment`)
                                                ) OR isnull(`Customer Segment`)
                                              )
                                              OR isnull(`Prev_Customer Segment`)
                                            )
                                            OR (
                                                 NOT(
                                                   SubCustSeg1 = Prev_SubCustSeg1)
                                               )
                                          )
                                          OR isnull(SubCustSeg1)
                                        )
                                        OR isnull(Prev_SubCustSeg1)
                                      )
                                      OR (
                                           NOT(
                                             SubCustSeg2 = Prev_SubCustSeg2)
                                         )
                                    )
                                    OR isnull(SubCustSeg2)
                                  )
                                  OR isnull(Prev_SubCustSeg2)
                                )
                                OR (
                                     NOT(
                                       SubCustSeg3 = Prev_SubCustSeg3)
                                   )
                              )
                              OR isnull(SubCustSeg3)
                            )
                            OR isnull(Prev_SubCustSeg3)
                          )
                          OR (
                               NOT(
                                 SubCustSeg4 = Prev_SubCustSeg4)
                             )
                        )
                        OR isnull(SubCustSeg4)
                      )
                      OR isnull(Prev_SubCustSeg4)
                    )
                    OR (
                         NOT(
                           SubCustSeg5 = Prev_SubCustSeg5)
                       )
                  )
                  OR isnull(SubCustSeg5)
                )
                OR isnull(Prev_SubCustSeg5)
              )
              OR (
                   NOT(
                     SubCustSeg6 = Prev_SubCustSeg6)
                 )
            )
            OR isnull(SubCustSeg6)
          )
          OR isnull(Prev_SubCustSeg6)
        )

),

GenerateRows_2796 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Filter_2795'], 
      '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "First_PosRevMonth", "dataType": "Date"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Last_PosRevMonth", "dataType": "Date"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]', 
      '1', 
      '(RowCount <= 2)', 
      '(RowCount + 1)', 
      'RowCount', 
      '100', 
      'recursive'
    )
  }}

),

Formula_2797_to_Formula_2798_0 AS (

  SELECT 
    (TO_DATE(NULL, 'yyyy-MM-dd')) AS Cohort,
    CAST(NULL AS INTEGER) AS `Cohort Tenure`,
    CAST(NULL AS INTEGER) AS `Cohort Active Flag`,
    CAST(NULL AS DOUBLE) AS `Cohort Gross Retention`,
    CAST(NULL AS DOUBLE) AS `Cohort Gross Customer Retention`,
    CAST(0 AS DOUBLE) AS Revenue,
    CAST('Customer Segment Migration' AS string) AS `Change Category`,
    CAST(0 AS DOUBLE) AS `Initial Revenue`,
    CAST(0 AS DOUBLE) AS Volume,
    * EXCEPT (`cohort gross customer retention`, 
    `cohort`, 
    `volume`, 
    `initial revenue`, 
    `cohort gross retention`, 
    `revenue`, 
    `cohort active flag`, 
    `cohort tenure`)
  
  FROM GenerateRows_2796 AS in0

),

AlteryxSelect_2800 AS (

  SELECT * EXCEPT (`RowCount`)
  
  FROM Formula_2797_to_Formula_2798_0 AS in0

),

Union_2799 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_2795_reject', 'Filter_2795', 'AlteryxSelect_2800'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "First_PosRevMonth", "dataType": "Date"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Last_PosRevMonth", "dataType": "Date"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "First_PosRevMonth", "dataType": "Date"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Last_PosRevMonth", "dataType": "Date"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Change Category", "dataType": "String"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "First_PosRevMonth", "dataType": "Date"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Last_PosRevMonth", "dataType": "Date"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_2799
