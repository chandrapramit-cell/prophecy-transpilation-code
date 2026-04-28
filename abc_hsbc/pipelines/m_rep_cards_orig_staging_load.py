from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_staging_load",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "''")
)

with Pipeline(args) as pipeline:
    cardsorigin = Process(
        name = "cardsorigin",
        properties = SFTPSource(
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = SFTPSource.SFTPSourceInternal(filePath = "path"),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/m_rep_cards_orig_staging_load/cardsorigin.yml")
        ),
        input_ports = None,
        is_custom_output_schema = True
    )
    m_rep_cards_orig_staging_load__rep_cards_orig_staging = Process(
        name = "m_rep_cards_orig_staging_load__REP_CARDS_ORIG_STAGING",
        properties = ModelTransform(modelName = "m_rep_cards_orig_staging_load__REP_CARDS_ORIG_STAGING")
    )
    cardsorigin >> m_rep_cards_orig_staging_load__rep_cards_orig_staging
