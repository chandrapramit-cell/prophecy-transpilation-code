from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_client_alias_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    dbattribute = Process(
        name = "DBATTRIBUTE",
        properties = Dataset(table = Dataset.DBTSource(name = "sf", sourceType = "Seed"), writeOptions = {"writeMode" : "overwrite"}),
        input_ports = None
    )
    exp_client_alias_sp_2 = Process(
        name = "EXP_CLIENT_ALIAS_SP_2",
        properties = CallStoredProc(
          storedProcedureIdentifier = "feed_number_generator1",
          parameters = {
            "feed_update_nbr__feed_number_generator1_sp_2__arg_1": "feed_update_nbr___sp_2__arg_1",
            "feed_update_nbr__feed_number_generator1_sp_2__arg_2": "feed_update_nbr___sp_2__arg_2",
            "feed_update_nbr__feed_number_generator1_sp_2__arg_3": "feed_update_nbr___sp_2__arg_3",
            "feed_update_nbr__feed_number_generator1_sp_2__arg_4": "feed_update_nbr___sp_2__arg_4"
          },
          passThroughColumns = []
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
    m_rep_cards_orig_client_alias_update__chooses = Process(
        name = "m_rep_cards_orig_client_alias_update__CHOOSES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__CHOOSES")
    )
    m_rep_cards_orig_client_alias_update__client_alias1 = Process(
        name = "m_rep_cards_orig_client_alias_update__CLIENT_ALIAS1",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__CLIENT_ALIAS1")
    )
    m_rep_cards_orig_client_alias_update__exp_client_alias_sp_pre_1_expr_7 = Process(
        name = "m_rep_cards_orig_client_alias_update__EXP_CLIENT_ALIAS_SP_PRE_1_EXPR_7",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__EXP_CLIENT_ALIAS_SP_PRE_1_EXPR_7")
    )
    m_rep_cards_orig_client_alias_update__resides = Process(
        name = "m_rep_cards_orig_client_alias_update__RESIDES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__RESIDES")
    )
    m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias = Process(
        name = "m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS"),
        input_ports = ["in_0", "in_1"]
    )
    (
        dbattribute._out(0)
        >> [m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias._in(0),
              m_rep_cards_orig_client_alias_update__exp_client_alias_sp_pre_1_expr_7._in(0)]
    )
    exp_client_alias_sp_2 >> m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias._in(1)
    (
        m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias._out(0)
        >> [m_rep_cards_orig_client_alias_update__chooses._in(0),
              m_rep_cards_orig_client_alias_update__client_alias1._in(0),
              m_rep_cards_orig_client_alias_update__resides._in(0)]
    )
    m_rep_cards_orig_client_alias_update__exp_client_alias_sp_pre_1_expr_7 >> exp_client_alias_sp_2
