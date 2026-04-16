from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_mmrt_month_end_table_truncate",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    m_mmrt_month_end_table_truncate__sm_credit_card_tran_mth_end = Process(
        name = "m_mmrt_month_end_table_truncate__SM_CREDIT_CARD_TRAN_MTH_END",
        properties = ModelTransform(modelName = "m_mmrt_month_end_table_truncate__SM_CREDIT_CARD_TRAN_MTH_END")
    )
    m_mmrt_month_end_table_truncate__sm_storecard_tran_mth_end = Process(
        name = "m_mmrt_month_end_table_truncate__SM_STORECARD_TRAN_MTH_END",
        properties = ModelTransform(modelName = "m_mmrt_month_end_table_truncate__SM_STORECARD_TRAN_MTH_END")
    )
    _11_0_1__database___generate_monthly_financialsm_mmrt_month_end_table_truncate_sq_dummy = Process(
        name = "{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_month_end_table_truncate_SQ_DUMMY",
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
            warehouseQuery = OracleSource.WarehouseQuery(query = "SELECT 'X' FROM DUAL")
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/m_mmrt_month_end_table_truncate/{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_month_end_table_truncate_SQ_DUMMY.yml"
          )
        ),
        input_ports = None,
        is_custom_output_schema = True
    )
    (
        _11_0_1__database___generate_monthly_financialsm_mmrt_month_end_table_truncate_sq_dummy._out(0)
        >> [m_mmrt_month_end_table_truncate__sm_credit_card_tran_mth_end._in(0),
              m_mmrt_month_end_table_truncate__sm_storecard_tran_mth_end._in(0)]
    )
