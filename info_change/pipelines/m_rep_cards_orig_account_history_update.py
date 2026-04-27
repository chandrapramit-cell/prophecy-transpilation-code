from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_account_history_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    dbattribute = Process(
        name = "DBATTRIBUTE",
        properties = Dataset(
          table = Dataset.DBTSource(name = "sp2", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    exp_get_dates_and_feed_number = Process(
        name = "EXP_GET_DATES_AND_FEED_NUMBER",
        properties = CallStoredProc(
          storedProcedureIdentifier = "prophecy-databricks-qa.Avpreet.sp1",
          parameters = {
            "arg1": "'U'",
            "arg2": "'360'",
            "arg3": "(CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1) THEN business_date_string__find_business_date_lkp_1 ELSE NULL END)",
            "arg4": "SUBSTRING((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1) THEN business_date_string__find_business_date_lkp_1 ELSE NULL END) , 0 , 8)"
          },
          passThroughColumns = [{"alias" : "ACCID_ACCOUNT_ALIAS", "expression" : {"expression" : "ACCID_ACCOUNT_ALIAS"}},
           {"alias" : "ACCID_TGT", "expression" : {"expression" : "ACCID_TGT"}},
           {"alias" : "APPLICATION_STORE_NUMBER_SRC", "expression" : {"expression" : "APPLICATION_STORE_NUMBER_SRC"}},
           {"alias" : "STATUS_DATE_SRC", "expression" : {"expression" : "STATUS_DATE_SRC"}},
           {"alias" : "STATUS_SRC", "expression" : {"expression" : "STATUS_SRC"}},
           {"alias" : "DOMICILE_BRANCH_TGT", "expression" : {"expression" : "DOMICILE_BRANCH_TGT"}},
           {"alias" : "STATUS_TGT", "expression" : {"expression" : "STATUS_TGT"}},
           {"alias" : "STATUS_DATE_TGT", "expression" : {"expression" : "STATUS_DATE_TGT"}},
           {"alias" : "EFROM_TGT", "expression" : {"expression" : "EFROM_TGT"}},
           {"alias" : "total_compare", "expression" : {"expression" : "total_compare"}},
           {
             "alias": "business_date",
             "expression": {
               "expression": "(CASE WHEN CAST(((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) IS NULL) AS BOOL) THEN (ERROR('No Business Date found on dbattriute')) ELSE (PARSE_TIMESTAMP('%Y%m%d', CAST(SUBSTRING((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) , 0 , 8) AS STRING))) END)"
             }
           },
           {
             "alias": "business_date_minus_1",
             "expression": {
               "expression": "(DATE_ADD((CASE WHEN CAST(((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) IS NULL) AS BOOL) THEN (ERROR('No Business Date found on dbattriute')) ELSE (PARSE_TIMESTAMP('%Y%m%d', CAST(SUBSTRING((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) , 0 , 8) AS STRING))) END)  ,INTERVAL -1  DAY))"
             }
           },
           {"alias" : "col5", "expression" : {"expression" : "1"}},
           {
             "alias": "feed_update_id",
             "expression": {
               "expression": "(CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1) THEN output_sp ELSE NULL END)"
             }
           }]
        )
    )
    feed_number_generator1 = Process(
        name = "feed_number_generator1",
        properties = CallStoredProc(
          storedProcedureIdentifier = "generate_feed_id",
          parameters = {"IN_SOURCE_SYSTEM_CODE" : "dummy"},
          passThroughColumns = []
        ),
        input_ports = None
    )
    m_rep_cards_orig_account_history_update__close_account_history_change = Process(
        name = "m_rep_cards_orig_account_history_update__CLOSE_ACCOUNT_HISTORY_CHANGE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__CLOSE_ACCOUNT_HISTORY_CHANGE")
    )
    m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_expr_1 = Process(
        name = "m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1")
    )
    m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_vars_0 = Process(
        name = "m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_VARS_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_VARS_0")
    )
    m_rep_cards_orig_account_history_update__ins_account_history = Process(
        name = "m_rep_cards_orig_account_history_update__INS_ACCOUNT_HISTORY",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__INS_ACCOUNT_HISTORY")
    )
    m_rep_cards_orig_account_history_update__new_maintains = Process(
        name = "m_rep_cards_orig_account_history_update__NEW_MAINTAINS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__NEW_MAINTAINS")
    )
    m_rep_cards_orig_account_history_update__open_account_history_change = Process(
        name = "m_rep_cards_orig_account_history_update__OPEN_ACCOUNT_HISTORY_CHANGE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__OPEN_ACCOUNT_HISTORY_CHANGE")
    )
    m_rep_cards_orig_account_history_update__rtr_account_history_change_expr_account_history_change = Process(
        name = "m_rep_cards_orig_account_history_update__RTR_ACCOUNT_HISTORY_CHANGE_EXPR_ACCOUNT_HISTORY_CHANGE",
        properties = ModelTransform(
          modelName = "m_rep_cards_orig_account_history_update__RTR_ACCOUNT_HISTORY_CHANGE_EXPR_ACCOUNT_HISTORY_CHANGE"
        )
    )
    m_rep_cards_orig_account_history_update__upd_account_history_same_date_change = Process(
        name = "m_rep_cards_orig_account_history_update__UPD_ACCOUNT_HISTORY_SAME_DATE_CHANGE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__UPD_ACCOUNT_HISTORY_SAME_DATE_CHANGE")
    )
    (
        m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_expr_1._out(0)
        >> [m_rep_cards_orig_account_history_update__rtr_account_history_change_expr_account_history_change._in(0),
              m_rep_cards_orig_account_history_update__upd_account_history_same_date_change._in(0),
              m_rep_cards_orig_account_history_update__ins_account_history._in(0),
              m_rep_cards_orig_account_history_update__new_maintains._in(0)]
    )
    m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_vars_0 >> exp_get_dates_and_feed_number
    exp_get_dates_and_feed_number >> m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_expr_1
    dbattribute >> m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_vars_0
    (
        m_rep_cards_orig_account_history_update__rtr_account_history_change_expr_account_history_change._out(0)
        >> [m_rep_cards_orig_account_history_update__close_account_history_change._in(0),
              m_rep_cards_orig_account_history_update__open_account_history_change._in(0)]
    )
