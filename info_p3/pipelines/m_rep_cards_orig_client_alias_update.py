from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_client_alias_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
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
    m_rep_cards_orig_client_alias_update__resides = Process(
        name = "m_rep_cards_orig_client_alias_update__RESIDES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__RESIDES")
    )
    m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias = Process(
        name = "m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS"),
        input_ports = None
    )
    m_rep_cards_orig_client_alias_update__find_business_date = Process(
        name = "m_rep_cards_orig_client_alias_update__find_business_date",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__find_business_date")
    )
    (
        m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias._out(0)
        >> [m_rep_cards_orig_client_alias_update__chooses._in(0),
              m_rep_cards_orig_client_alias_update__client_alias1._in(0),
              m_rep_cards_orig_client_alias_update__resides._in(0)]
    )
