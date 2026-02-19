{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sample_143 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Sample_143')}}

),

Summarize_144 AS (

  SELECT 
    DISTINCT MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    YearMonth AS YearMonth,
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    Avg_TotalRVU AS Avg_TotalRVU
  
  FROM Sample_143 AS in0

),

Summarize_145 AS (

  SELECT 
    (
      (
        CASE
          WHEN ((COUNT(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) > 0)
            THEN (
              COALESCE(
                (SUM(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)), 
                0)
            )
          ELSE NULL
        END
      )
      / (COUNT(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
    ) AS Avg_Avg_TotalRVU,
    POWER(
      (
        (
          (
            CASE
              WHEN ((COUNT((Avg_TotalRVU * Avg_TotalRVU)) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) > 0)
                THEN (
                  COALESCE(
                    (SUM((Avg_TotalRVU * Avg_TotalRVU)) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)), 
                    0)
                )
              ELSE NULL
            END
          )
          - (
              (
                (
                  CASE
                    WHEN ((COUNT(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) > 0)
                      THEN (
                        COALESCE(
                          (SUM(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)), 
                          0)
                      )
                    ELSE NULL
                  END
                )
                * (
                    CASE
                      WHEN ((COUNT(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) > 0)
                        THEN (
                          COALESCE(
                            (SUM(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)), 
                            0)
                        )
                      ELSE NULL
                    END
                  )
              )
              / CAST((COUNT(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS FLOAT64)
            )
        )
        / (
            CASE
              WHEN ((COUNT(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1)
                THEN NULL
              ELSE (
                (COUNT(Avg_TotalRVU) OVER (PARTITION BY MBR_AGE_RANGE, YearMonth ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
                - 1
              )
            END
          )
      ), 
      0.5) AS StdDev_Avg_TotalRVU,
    *
  
  FROM Summarize_144 AS in0

),

Join_146_inner_formula_to_Formula_148_0 AS (

  SELECT 
    ((Avg_TotalRVU - Avg_Avg_TotalRVU) / StdDev_Avg_TotalRVU) AS `Standardized RVU`,
    *
  
  FROM Summarize_145 AS in0

),

Join_146_inner_formula_to_Formula_148_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`Standardized RVU` <= 0)
          THEN '1. Low      '
        WHEN (`Standardized RVU` <= 1)
          THEN '2. Moderate '
        WHEN (`Standardized RVU` <= 3)
          THEN '3. High     '
        WHEN (`Standardized RVU` > 3)
          THEN '4. Very High'
        ELSE '99999       '
      END
    ) AS STRING) AS Bucket,
    *
  
  FROM Join_146_inner_formula_to_Formula_148_0 AS in0

)

SELECT *

FROM Join_146_inner_formula_to_Formula_148_1
