from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_sunday_check",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    m_rep_sunday_check__rep_inf_workflow = Process(
        name = "m_rep_sunday_check__REP_INF_WORKFLOW",
        properties = ModelTransform(modelName = "m_rep_sunday_check__REP_INF_WORKFLOW")
    )
    _11_0_1__database___generate_monthly_financialsm_rep_sunday_check_sq_dbattribute = Process(
        name = "{11_0_1}_Database___Generate_Monthly_Financialsm_rep_sunday_check_SQ_DBATTRIBUTE",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "$$jdbcUrl",
              "username": "$$username",
              "id": "transpiled_connection",
              "port": "1521",
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = OracleSource.OracleSourceInternal(
            pathSelection = "warehouseQuery",
            tableFullName = OracleSource.WarehouseTableName(
              schema = {
                "type": "concat_operation",
                "properties": {
                  "elements": [{"type" : "config", "properties" : {"kind" : "prophecy", "name" : "SCHEMA"}}]
                }
              },
              name = "MinervaRepository"
            ),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT /*+ parallel(db) */ rtrim(to_char(to_date(currvalue, 'YYYYMMDD'), 'DAY')) AS day_of_week FROM dbattribute db WHERE name = 'businessdate' AND DAY_OF_WEEK != 'THURSDAY' AND DAY_OF_WEEK != 'FRIDAY' AND DAY_OF_WEEK != 'SUNDAY' AND DAY_OF_WEEK != 'MONDAY' AND DAY_OF_WEEK != 'TUESDAY' AND DAY_OF_WEEK != 'WEDNESDAY'"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/m_rep_sunday_check/{11_0_1}_Database___Generate_Monthly_Financialsm_rep_sunday_check_SQ_DBATTRIBUTE.yml"
          )
        ),
        input_ports = None,
        is_custom_output_schema = True
    )
    (
        _11_0_1__database___generate_monthly_financialsm_rep_sunday_check_sq_dbattribute
        >> m_rep_sunday_check__rep_inf_workflow
    )
