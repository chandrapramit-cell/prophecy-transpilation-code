from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_client_staging_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    chooses1 = Process(
        name = "CHOOSES1",
        properties = Dataset(
          table = Dataset.DBTSource(name = "CHOOSES1", sourceType = "UnreferencedSource"),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    dbreference = Process(
        name = "DBreference",
        properties = Dataset(
          table = Dataset.DBTSource(name = "dsg", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    dbreference = Process(
        name = "DBreference",
        properties = Dataset(
          table = Dataset.DBTSource(name = "dsg", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    exp_cardsorig_client_sp = Process(
        name = "EXP_CARDSORIG_CLIENT_sp",
        properties = CallStoredProc(
          storedProcedureIdentifier = "feed_number_generator1",
          parameters = {
            "arg1": "'U'",
            "arg2": "'360'",
            "arg3": "lag(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_3 ELSE cast(null as STRING) END) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST)",
            "arg4": "substring(lag(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_3 ELSE cast(null as STRING) END) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST), 1, 8)"
          },
          passThroughColumns = [{"alias" : "INITIALS", "expression" : {"expression" : "INITIALS"}},
           {"alias" : "TITLE", "expression" : {"expression" : "TITLE"}},
           {"alias" : "FORENAMES", "expression" : {"expression" : "FORENAMES"}},
           {"alias" : "SURNAME", "expression" : {"expression" : "SURNAME"}},
           {"alias" : "GENDER", "expression" : {"expression" : "GENDER"}},
           {"alias" : "prophecy_sk", "expression" : {"expression" : "prophecy_sk"}},
           {"alias" : "CLIENT_STATUS_LKP", "expression" : {"expression" : "CLIENT_STATUS_LKP"}},
           {"alias" : "DOB", "expression" : {"expression" : "DOB"}},
           {"alias" : "CLID", "expression" : {"expression" : "CLID"}},
           {"alias" : "MERGE_CLID", "expression" : {"expression" : "MERGE_CLID"}},
           {"alias" : "EFROM", "expression" : {"expression" : "EFROM"}},
           {"alias" : "CUSTOMER_NUMBER", "expression" : {"expression" : "CUSTOMER_NUMBER"}},
           {"alias" : "CLID_LKP", "expression" : {"expression" : "CLID_LKP"}},
           {"alias" : "MERGE_CLID_LKP", "expression" : {"expression" : "MERGE_CLID_LKP"}},
           {"alias" : "EFROM_LKP", "expression" : {"expression" : "EFROM_LKP"}},
           {"alias" : "TITLE_LKP", "expression" : {"expression" : "TITLE_LKP"}},
           {"alias" : "FORENAMES_LKP", "expression" : {"expression" : "FORENAMES_LKP"}},
           {"alias" : "SURNAME_LKP", "expression" : {"expression" : "SURNAME_LKP"}},
           {"alias" : "GENDER_LKP", "expression" : {"expression" : "GENDER_LKP"}},
           {"alias" : "DOB_LKP", "expression" : {"expression" : "DOB_LKP"}},
           {"alias" : "lookup_string", "expression" : {"expression" : "lookup_string"}},
           {
             "alias": "business_date_string__find_business_date_lkp_3",
             "expression": {"expression" : "business_date_string__find_business_date_lkp_3"}
           },
           {"alias" : "LOOKUP_VARIABLE_8", "expression" : {"expression" : "LOOKUP_VARIABLE_8"}}]
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
    m_rep_cards_orig_client_staging_update__chooses = Process(
        name = "m_rep_cards_orig_client_staging_update__CHOOSES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__CHOOSES")
    )
    m_rep_cards_orig_client_staging_update__chooses1_exp = Process(
        name = "m_rep_cards_orig_client_staging_update__CHOOSES1_EXP",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__CHOOSES1_EXP"),
        input_ports = None
    )
    m_rep_cards_orig_client_staging_update__client_staging_existing_client_diff_date = Process(
        name = "m_rep_cards_orig_client_staging_update__CLIENT_STAGING_EXISTING_CLIENT_DIFF_DATE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__CLIENT_STAGING_EXISTING_CLIENT_DIFF_DATE")
    )
    m_rep_cards_orig_client_staging_update__client_staging_new_client = Process(
        name = "m_rep_cards_orig_client_staging_update__CLIENT_STAGING_NEW_CLIENT",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__CLIENT_STAGING_NEW_CLIENT")
    )
    m_rep_cards_orig_client_staging_update__client_staging_update_same_day = Process(
        name = "m_rep_cards_orig_client_staging_update__CLIENT_STAGING_UPDATE_SAME_DAY",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__CLIENT_STAGING_UPDATE_SAME_DAY")
    )
    m_rep_cards_orig_client_staging_update__exp_cardsorig_client_expr_30 = Process(
        name = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT_EXPR_30",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT_EXPR_30")
    )
    m_rep_cards_orig_client_staging_update__exp_cardsorig_client_vars_0 = Process(
        name = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT_VARS_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT_VARS_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    m_rep_cards_orig_client_staging_update__exp_cardsorig_client_reformat = Process(
        name = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT_reformat",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT_reformat")
    )
    m_rep_cards_orig_client_staging_update__minerva_exception_new_record = Process(
        name = "m_rep_cards_orig_client_staging_update__MINERVA_EXCEPTION_NEW_RECORD",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__MINERVA_EXCEPTION_NEW_RECORD")
    )
    m_rep_cards_orig_client_staging_update__miss_to_mrs_client = Process(
        name = "m_rep_cards_orig_client_staging_update__MISS_TO_MRS_CLIENT",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__MISS_TO_MRS_CLIENT")
    )
    m_rep_cards_orig_client_staging_update__rtr_cardsorig_client_expr_same_client_different_date_insert = Process(
        name = "m_rep_cards_orig_client_staging_update__RTR_CARDSORIG_CLIENT_EXPR_SAME_CLIENT_DIFFERENT_DATE_INSERT",
        properties = ModelTransform(
          modelName = "m_rep_cards_orig_client_staging_update__RTR_CARDSORIG_CLIENT_EXPR_SAME_CLIENT_DIFFERENT_DATE_INSERT"
        )
    )
    m_rep_cards_orig_client_staging_update__seq_clid_0 = Process(
        name = "m_rep_cards_orig_client_staging_update__SEQ_clid_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__SEQ_clid_0"),
        input_ports = None
    )
    m_rep_cards_orig_client_staging_update__lookup_dbref_value4 = Process(
        name = "m_rep_cards_orig_client_staging_update__lookup_dbref_value4",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__lookup_dbref_value4")
    )
    (
        m_rep_cards_orig_client_staging_update__exp_cardsorig_client_expr_30._out(0)
        >> [m_rep_cards_orig_client_staging_update__client_staging_update_same_day._in(0),
              m_rep_cards_orig_client_staging_update__client_staging_new_client._in(0),
              m_rep_cards_orig_client_staging_update__rtr_cardsorig_client_expr_same_client_different_date_insert._in(0),
              m_rep_cards_orig_client_staging_update__chooses._in(0)]
    )
    dbreference >> m_rep_cards_orig_client_staging_update__exp_cardsorig_client_vars_0._in(2)
    exp_cardsorig_client_sp >> m_rep_cards_orig_client_staging_update__exp_cardsorig_client_reformat
    (
        dbreference._out(0)
        >> [m_rep_cards_orig_client_staging_update__exp_cardsorig_client_vars_0._in(0),
              m_rep_cards_orig_client_staging_update__exp_cardsorig_client_vars_0._in(1)]
    )
    (
        m_rep_cards_orig_client_staging_update__exp_cardsorig_client_reformat._out(0)
        >> [m_rep_cards_orig_client_staging_update__exp_cardsorig_client_expr_30._in(0),
              m_rep_cards_orig_client_staging_update__minerva_exception_new_record._in(0)]
    )
    (
        m_rep_cards_orig_client_staging_update__rtr_cardsorig_client_expr_same_client_different_date_insert._out(0)
        >> [m_rep_cards_orig_client_staging_update__client_staging_existing_client_diff_date._in(0),
              m_rep_cards_orig_client_staging_update__miss_to_mrs_client._in(0)]
    )
    m_rep_cards_orig_client_staging_update__exp_cardsorig_client_vars_0 >> exp_cardsorig_client_sp
