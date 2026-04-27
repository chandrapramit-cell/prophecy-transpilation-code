from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_account_update",
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
    exp_account_change = Process(
        name = "EXP_ACCOUNT_CHANGE",
        properties = CallStoredProc(
          storedProcedureIdentifier = "prophecy-databricks-qa.Avpreet.sp1",
          parameters = {
            "arg1": "'U'",
            "arg2": "'360'",
            "arg3": "(CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1) THEN business_date_string__find_business_date_lkp_1 ELSE NULL END)",
            "arg4": "SUBSTRING((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1) THEN business_date_string__find_business_date_lkp_1 ELSE NULL END) , 0 , 8)"
          },
          passThroughColumns = [{"alias" : "APPLICATION_NUMBER", "expression" : {"expression" : "APPLICATION_NUMBER"}},
           {
             "alias": "business_date",
             "expression": {
               "expression": "(CASE WHEN CAST(((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) IS NULL) AS BOOL) THEN (ERROR('No Business Date found on dbattriute')) ELSE (PARSE_TIMESTAMP('%Y%m%d', CAST(SUBSTRING((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) , 0 , 8) AS STRING))) END)"
             }
           },
           {
             "alias": "feed_update_id",
             "expression": {
               "expression": "(CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1) THEN output_sp ELSE NULL END)"
             }
           },
           {"alias" : "SOURCE_SYSTEM_CODE", "expression" : {"expression" : "'U'"}},
           {"alias" : "SOURCE_TYPE", "expression" : {"expression" : "0"}},
           {"alias" : "HOLDS_CLASS", "expression" : {"expression" : "270"}},
           {"alias" : "o_comp_acc_type1", "expression" : {"expression" : "o_comp_acc_type1"}},
           {"alias" : "o_comp_source_code1", "expression" : {"expression" : "o_comp_source_code1"}},
           {"alias" : "o_comp_store_number1", "expression" : {"expression" : "o_comp_store_number1"}},
           {"alias" : "o_compare_status1", "expression" : {"expression" : "o_compare_status1"}},
           {"alias" : "o_compare_status_date1", "expression" : {"expression" : "o_compare_status_date1"}},
           {
             "alias": "o_DATE_LAST_ACC_STATUS_CHANGE_TGT",
             "expression": {
               "expression": "(CASE WHEN (O_COMPARE_STATUS1 = 1) THEN (CASE WHEN CAST(((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) IS NULL) AS BOOL) THEN (ERROR('No Business Date found on dbattriute')) ELSE (PARSE_TIMESTAMP('%Y%m%d', CAST(SUBSTRING((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) , 0 , 8) AS STRING))) END) ELSE DATE_LAST_ACC_STATUS_CHANGE_TGT1 END)"
             }
           },
           {
             "alias": "TOTAL_COMPARE",
             "expression": {
               "expression": "((((o_comp_acc_type1 + o_comp_store_number1) + o_comp_source_code1) + o_compare_status1) + o_compare_status_date1)"
             }
           },
           {"alias" : "prophecy_sk", "expression" : {"expression" : "prophecy_sk"}}]
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
    m_rep_cards_orig_account_update__exp_account_change_vars_0 = Process(
        name = "m_rep_cards_orig_account_update__EXP_ACCOUNT_CHANGE_VARS_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_update__EXP_ACCOUNT_CHANGE_VARS_0"),
        input_ports = ["in_0", "in_1"]
    )
    m_rep_cards_orig_account_update__exp_account_compare = Process(
        name = "m_rep_cards_orig_account_update__EXP_ACCOUNT_COMPARE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_update__EXP_ACCOUNT_COMPARE"),
        input_ports = None
    )
    m_rep_cards_orig_account_update__holds = Process(
        name = "m_rep_cards_orig_account_update__HOLDS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_update__HOLDS")
    )
    m_rep_cards_orig_account_update__minerva_exception = Process(
        name = "m_rep_cards_orig_account_update__MINERVA_EXCEPTION",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_update__MINERVA_EXCEPTION")
    )
    m_rep_cards_orig_account_update__new_account = Process(
        name = "m_rep_cards_orig_account_update__NEW_ACCOUNT",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_update__NEW_ACCOUNT")
    )
    m_rep_cards_orig_account_update__new_chooses = Process(
        name = "m_rep_cards_orig_account_update__NEW_CHOOSES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_update__NEW_CHOOSES")
    )
    m_rep_cards_orig_account_update__new_maintains = Process(
        name = "m_rep_cards_orig_account_update__NEW_MAINTAINS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_update__NEW_MAINTAINS")
    )
    m_rep_cards_orig_account_update__rtr_account_join_expr_25 = Process(
        name = "m_rep_cards_orig_account_update__RTR_ACCOUNT_JOIN_EXPR_25",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_update__RTR_ACCOUNT_JOIN_EXPR_25"),
        input_ports = ["in_0", "in_1"]
    )
    m_rep_cards_orig_account_update__upd_account = Process(
        name = "m_rep_cards_orig_account_update__UPD_ACCOUNT",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_update__UPD_ACCOUNT")
    )
    (
        m_rep_cards_orig_account_update__rtr_account_join_expr_25._out(0)
        >> [m_rep_cards_orig_account_update__new_account._in(0), m_rep_cards_orig_account_update__upd_account._in(0),
              m_rep_cards_orig_account_update__minerva_exception._in(0),
              m_rep_cards_orig_account_update__new_maintains._in(0),
              m_rep_cards_orig_account_update__holds._in(0),
              m_rep_cards_orig_account_update__new_chooses._in(0)]
    )
    dbattribute >> m_rep_cards_orig_account_update__exp_account_change_vars_0._in(0)
    m_rep_cards_orig_account_update__exp_account_change_vars_0 >> exp_account_change
    exp_account_change >> m_rep_cards_orig_account_update__rtr_account_join_expr_25._in(1)
    (
        m_rep_cards_orig_account_update__exp_account_compare._out(0)
        >> [m_rep_cards_orig_account_update__rtr_account_join_expr_25._in(0),
              m_rep_cards_orig_account_update__exp_account_change_vars_0._in(1)]
    )
