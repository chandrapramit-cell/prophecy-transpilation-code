from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_client_preference_update",
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
    m_rep_cards_orig_client_preference_update__exp_cards_orig_client_preference = Process(
        name = "m_rep_cards_orig_client_preference_update__EXP_CARDS_ORIG_CLIENT_PREFERENCE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_preference_update__EXP_CARDS_ORIG_CLIENT_PREFERENCE")
    )
    m_rep_cards_orig_client_preference_update__ins_client_preference = Process(
        name = "m_rep_cards_orig_client_preference_update__INS_CLIENT_PREFERENCE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_preference_update__INS_CLIENT_PREFERENCE")
    )
    m_rep_cards_orig_client_preference_update__upd_client_preference = Process(
        name = "m_rep_cards_orig_client_preference_update__UPD_CLIENT_PREFERENCE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_preference_update__UPD_CLIENT_PREFERENCE")
    )
    dbattribute >> m_rep_cards_orig_client_preference_update__exp_cards_orig_client_preference
    (
        m_rep_cards_orig_client_preference_update__exp_cards_orig_client_preference._out(0)
        >> [m_rep_cards_orig_client_preference_update__upd_client_preference._in(0),
              m_rep_cards_orig_client_preference_update__ins_client_preference._in(0)]
    )
