{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH R_64_12_out0 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('TS_Factory_Sample', 'R_64_12', 'out0') }}

),

DynamicRename_82_12 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['R_64_12'], 
      [
        'q_coefficients_for_seasonal_models_1', 
        'q_coefficients_1', 
        'p_value', 
        'ME', 
        'MPE', 
        'Intercept', 
        'RMSE', 
        'MAE', 
        'bic', 
        'Degrees_of_freedom', 
        'D_seasonal', 
        'aicc', 
        'MAPE', 
        'variableGroup', 
        'q', 
        'MASE', 
        'p', 
        'X_squared_statistic_Ljung_Box', 
        'p_coefficients_1', 
        'Sigma_2', 
        'ACF1', 
        'p_coefficients_for_seasonal_models_1', 
        'Q_seasonal', 
        'loglik', 
        'P_seasonal', 
        'aic', 
        'd'
      ], 
      'advancedRename', 
      [
        'q_coefficients_for_seasonal_models_1', 
        'q_coefficients_1', 
        'p_value', 
        'ME', 
        'MPE', 
        'Intercept', 
        'RMSE', 
        'MAE', 
        'bic', 
        'Degrees_of_freedom', 
        'D_seasonal', 
        'aicc', 
        'MAPE', 
        'q', 
        'MASE', 
        'variableGroup', 
        'p', 
        'X_squared_statistic_Ljung_Box', 
        'p_coefficients_1', 
        'Sigma_2', 
        'ACF1', 
        'p_coefficients_for_seasonal_models_1', 
        'Q_seasonal', 
        'loglik', 
        'P_seasonal', 
        'aic', 
        'd'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '_', ' ')"
    )
  }}

),

DynamicRename_83_12_before AS (

  SELECT 
    CAST(NULL AS string) AS sigma__2,
    *
  
  FROM DynamicRename_82_12 AS in0

),

DynamicRename_83_12 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_83_12_before'], 
      [
        'p coefficients 1', 
        'sigma__2', 
        'Sigma 2', 
        'q coefficients 1', 
        'p coefficients for seasonal models 1', 
        'q coefficients for seasonal models 1'
      ], 
      'advancedRename', 
      [
        'sigma__2', 
        'Q seasonal', 
        'ME', 
        'MPE', 
        'Intercept', 
        'RMSE', 
        'p value', 
        'MAE', 
        'bic', 
        'D seasonal', 
        'P seasonal', 
        'p coefficients 1', 
        'aicc', 
        'MAPE', 
        'q', 
        'Degrees of freedom', 
        'MASE', 
        'variableGroup', 
        'p', 
        'Sigma 2', 
        'X squared statistic Ljung Box', 
        'q coefficients 1', 
        'p coefficients for seasonal models 1', 
        'q coefficients for seasonal models 1', 
        'ACF1', 
        'loglik', 
        'aic', 
        'd'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '.', '^')"
    )
  }}

),

DynamicRename_83_12_after AS (

  SELECT 
    CAST(NULL AS string) AS sigma__2,
    *
  
  FROM DynamicRename_83_12 AS in0

),

DynamicRename_84_12_before AS (

  SELECT 
    CAST(NULL AS string) AS p__coefficients__1,
    CAST(NULL AS string) AS q__coefficients__1,
    CAST(NULL AS string) AS q__coefficients__2,
    CAST(NULL AS string) AS p__coefficients__for__seasonal__models__1,
    CAST(NULL AS string) AS q__coefficients__for__seasonal__models__1,
    CAST(NULL AS string) AS Coefficients__of__covariates__1,
    CAST(NULL AS string) AS Coefficients__of__covariates__2,
    *
  
  FROM DynamicRename_83_12_after AS in0

),

DynamicRename_84_12 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_84_12_before'], 
      [
        'Q seasonal', 
        'ME', 
        'MPE', 
        'Intercept', 
        'RMSE', 
        'q__coefficients__2', 
        'p value', 
        'MAE', 
        'p__coefficients__for__seasonal__models__1', 
        'bic', 
        'D seasonal', 
        'p__coefficients__1', 
        'Coefficients__of__covariates__2', 
        'q__coefficients__for__seasonal__models__1', 
        'P seasonal', 
        'p coefficients 1', 
        'aicc', 
        'MAPE', 
        'sigma__2', 
        'variableGroup', 
        'q', 
        'Degrees of freedom', 
        'MASE', 
        'p', 
        'Coefficients__of__covariates__1', 
        'Sigma 2', 
        'X squared statistic Ljung Box', 
        'q coefficients 1', 
        'p coefficients for seasonal models 1', 
        'q coefficients for seasonal models 1', 
        'q__coefficients__1', 
        'ACF1', 
        'loglik', 
        'aic', 
        'd'
      ], 
      'advancedRename', 
      [
        'p__coefficients__1', 
        'q__coefficients__1', 
        'q__coefficients__2', 
        'p__coefficients__for__seasonal__models__1', 
        'q__coefficients__for__seasonal__models__1', 
        'Coefficients__of__covariates__1', 
        'Coefficients__of__covariates__2', 
        'sigma__2', 
        'Q seasonal', 
        'ME', 
        'MPE', 
        'Intercept', 
        'RMSE', 
        'p value', 
        'MAE', 
        'bic', 
        'D seasonal', 
        'P seasonal', 
        'p coefficients 1', 
        'aicc', 
        'MAPE', 
        'q', 
        'Degrees of freedom', 
        'MASE', 
        'variableGroup', 
        'p', 
        'Sigma 2', 
        'X squared statistic Ljung Box', 
        'q coefficients 1', 
        'p coefficients for seasonal models 1', 
        'q coefficients for seasonal models 1', 
        'ACF1', 
        'loglik', 
        'aic', 
        'd'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '.', ' ')"
    )
  }}

),

DynamicRename_84_12_after AS (

  SELECT 
    CAST(NULL AS string) AS p__coefficients__1,
    CAST(NULL AS string) AS q__coefficients__1,
    CAST(NULL AS string) AS q__coefficients__2,
    CAST(NULL AS string) AS p__coefficients__for__seasonal__models__1,
    CAST(NULL AS string) AS q__coefficients__for__seasonal__models__1,
    CAST(NULL AS string) AS Coefficients__of__covariates__1,
    CAST(NULL AS string) AS Coefficients__of__covariates__2,
    *
  
  FROM DynamicRename_84_12 AS in0

)

SELECT *

FROM DynamicRename_84_12_after
