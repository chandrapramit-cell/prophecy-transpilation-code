from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_client_flag_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    dbattribute = Process(
        name = "DBATTRIBUTE",
        properties = Dataset(table = Dataset.DBTSource(name = "s1", sourceType = "Seed"), writeOptions = {"writeMode" : "overwrite"}),
        input_ports = None
    )
    exp_client_flag_sp = Process(
        name = "EXP_CLIENT_FLAG_sp",
        properties = CallStoredProc(
          storedProcedureIdentifier = "sp1",
          parameters = {
            "arg1": "'U'",
            "arg2": "'360'",
            "arg3": "CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END",
            "arg4": "substring(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END, 1, 8)"
          },
          passThroughColumns = [{"alias" : "CLID", "expression" : {"expression" : "CLID"}},
           {"alias" : "MERGE_CLID", "expression" : {"expression" : "MERGE_CLID"}},
           {"alias" : "APPLICATION_STATUS", "expression" : {"expression" : "APPLICATION_STATUS"}},
           {"alias" : "LAST_ACTION_DATE", "expression" : {"expression" : "LAST_ACTION_DATE"}},
           {"alias" : "lookup_string", "expression" : {"expression" : "lookup_string"}},
           {
             "alias": "business_date_string__find_business_date_lkp_1",
             "expression": {"expression" : "business_date_string__find_business_date_lkp_1"}
           },
           {"alias" : "LOOKUP_VARIABLE_3", "expression" : {"expression" : "LOOKUP_VARIABLE_3"}}]
        )
    )
    m_rep_cards_orig_client_flag_update__client_flag = Process(
        name = "m_rep_cards_orig_client_flag_update__CLIENT_FLAG",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_flag_update__CLIENT_FLAG")
    )
    m_rep_cards_orig_client_flag_update__exp_client_flag_vars_0 = Process(
        name = "m_rep_cards_orig_client_flag_update__EXP_CLIENT_FLAG_VARS_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_flag_update__EXP_CLIENT_FLAG_VARS_0")
    )
    m_rep_cards_orig_client_flag_update__exp_client_flag_reformat = Process(
        name = "m_rep_cards_orig_client_flag_update__EXP_CLIENT_FLAG_reformat",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_flag_update__EXP_CLIENT_FLAG_reformat")
    )
    m_rep_cards_orig_client_flag_update__ins_chooses = Process(
        name = "m_rep_cards_orig_client_flag_update__INS_CHOOSES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_flag_update__INS_CHOOSES")
    )
    exp_client_flag_sp >> m_rep_cards_orig_client_flag_update__exp_client_flag_reformat
    (
        m_rep_cards_orig_client_flag_update__exp_client_flag_reformat._out(0)
        >> [m_rep_cards_orig_client_flag_update__client_flag._in(0),
              m_rep_cards_orig_client_flag_update__ins_chooses._in(0)]
    )
    dbattribute >> m_rep_cards_orig_client_flag_update__exp_client_flag_vars_0
    m_rep_cards_orig_client_flag_update__exp_client_flag_vars_0 >> exp_client_flag_sp
