from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_address_staging",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    m_rep_cards_orig_address_staging__address_staging = Process(
        name = "m_rep_cards_orig_address_staging__ADDRESS_STAGING",
        properties = ModelTransform(modelName = "m_rep_cards_orig_address_staging__ADDRESS_STAGING")
    )

