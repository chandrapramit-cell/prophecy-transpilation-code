from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_account_alias_update",
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
    m_rep_cards_orig_account_alias_update__maintains = Process(
        name = "m_rep_cards_orig_account_alias_update__MAINTAINS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_alias_update__MAINTAINS")
    )
    m_rep_cards_orig_account_alias_update__new_account_alias = Process(
        name = "m_rep_cards_orig_account_alias_update__NEW_ACCOUNT_ALIAS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_alias_update__NEW_ACCOUNT_ALIAS")
    )
    m_rep_cards_orig_account_alias_update__rtr_fortrav_account_alias_expr_new_account_alias = Process(
        name = "m_rep_cards_orig_account_alias_update__RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS",
        properties = ModelTransform(
          modelName = "m_rep_cards_orig_account_alias_update__RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS"
        ),
        input_ports = None
    )
    m_rep_cards_orig_account_alias_update__find_business_date = Process(
        name = "m_rep_cards_orig_account_alias_update__find_business_date",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_alias_update__find_business_date")
    )
    (
        m_rep_cards_orig_account_alias_update__rtr_fortrav_account_alias_expr_new_account_alias._out(0)
        >> [m_rep_cards_orig_account_alias_update__new_account_alias._in(0),
              m_rep_cards_orig_account_alias_update__maintains._in(0)]
    )
