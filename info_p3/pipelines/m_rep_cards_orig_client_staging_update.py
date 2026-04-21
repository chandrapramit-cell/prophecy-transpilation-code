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
    m_rep_cards_orig_client_staging_update__exp_cardsorig_client = Process(
        name = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    m_rep_cards_orig_client_staging_update__exp_cardsorig_client_expr_12 = Process(
        name = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT_EXPR_12",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT_EXPR_12")
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
    m_rep_cards_orig_client_staging_update__find_business_date = Process(
        name = "m_rep_cards_orig_client_staging_update__find_business_date",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_staging_update__find_business_date")
    )
    (
        m_rep_cards_orig_client_staging_update__rtr_cardsorig_client_expr_same_client_different_date_insert._out(0)
        >> [m_rep_cards_orig_client_staging_update__client_staging_existing_client_diff_date._in(0),
              m_rep_cards_orig_client_staging_update__miss_to_mrs_client._in(0)]
    )
    (
        m_rep_cards_orig_client_staging_update__exp_cardsorig_client_expr_12._out(0)
        >> [m_rep_cards_orig_client_staging_update__chooses._in(0),
              m_rep_cards_orig_client_staging_update__client_staging_new_client._in(0),
              m_rep_cards_orig_client_staging_update__client_staging_update_same_day._in(0),
              m_rep_cards_orig_client_staging_update__rtr_cardsorig_client_expr_same_client_different_date_insert._in(0)]
    )
    (
        m_rep_cards_orig_client_staging_update__exp_cardsorig_client._out(0)
        >> [m_rep_cards_orig_client_staging_update__exp_cardsorig_client_expr_12._in(0),
              m_rep_cards_orig_client_staging_update__minerva_exception_new_record._in(0)]
    )
