from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_ohc_account_transfer_staging",
    version = 1,
    auto_layout = False,
    params = Parameters(username = "''", SCHEMA = "''", InputFile_fsk2tfr = "''", jdbcUrl = "''", password = "''")
)

with Pipeline(args) as pipeline:
    account_transfer = Process(
        name = "account_transfer",
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
          properties = SFTPSource.SFTPSourceInternal(
            filePath = {
              "type": "concat_operation",
              "properties": {
                "elements": [{"type" : "config", "properties" : {"kind" : "prophecy", "name" : "InputFile_fsk2tfr"}}]
              }
            }
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/m_rep_ohc_account_transfer_staging/account_transfer.yml")
        ),
        input_ports = None,
        is_custom_output_schema = True
    )
    m_rep_ohc_account_transfer_staging__rep_g2_account_tfr_staging = Process(
        name = "m_rep_ohc_account_transfer_staging__REP_G2_ACCOUNT_TFR_STAGING",
        properties = ModelTransform(modelName = "m_rep_ohc_account_transfer_staging__REP_G2_ACCOUNT_TFR_STAGING")
    )
    account_transfer >> m_rep_ohc_account_transfer_staging__rep_g2_account_tfr_staging
