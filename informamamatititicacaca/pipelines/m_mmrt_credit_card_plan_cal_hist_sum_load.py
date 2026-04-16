from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_mmrt_credit_card_plan_cal_hist_sum_load",
    version = 1,
    auto_layout = False,
    params = Parameters(MP_BUSDATE = "''", username = "''", SCHEMA = "'SCHEMA'", jdbcUrl = "''", password = "''")
)

with Pipeline(args) as pipeline:
    m_mmrt_credit_card_plan_cal_hist_sum_load__sm_credit_card_plan_cal_mth_his = Process(
        name = "m_mmrt_credit_card_plan_cal_hist_sum_load__SM_CREDIT_CARD_PLAN_CAL_MTH_HIS",
        properties = ModelTransform(modelName = "m_mmrt_credit_card_plan_cal_hist_sum_load__SM_CREDIT_CARD_PLAN_CAL_MTH_HIS")
    )
    _11_0_1__database___generate_monthly_financialsm_mmrt_credit_card_plan_cal_hist_sum_load_sq_credit_plan = Process(
        name = "{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_credit_card_plan_cal_hist_sum_load_SQ_CREDIT_PLAN",
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
              query = "SELECT /*+ parallel(AMA) parallel(PLB) parallel(PLI) use_hash(AMA PLB PLI) */   \r        \tSUBSTR('$$MP_BUSDATE',1,6) PERIODID,\r        \tAMA.ACC_NUM,\r        \tPLI.APR_1,\r        \tPLB.CURRENT_BALANCE,\r        \tPLB.PLAN_NUMBER,\r        \tPLB.PLAN_TYPE \r FROM   \r \tALL_MONTHLY_ACCOUNTS AMA, \r        \tCREDIT_PLAN_BALANCE PLB,\r        \tCREDIT_PLAN_INTEREST PLI\r WHERE  \r \tAMA.ACCID = PLB.ACCID\r AND\tAMA.ACCID = PLI.ACCID\r AND\tPLB.PLAN_NUMBER = PLI.PLAN_NUMBER\r AND\tPLB.PLAN_DATE = PLI.PLAN_DATE\r AND    \tAMA.SOURCE_SYSTEM_CODE = 'G'\r AND    \tAMA.ACC_TYPE           IN ('410', '420', '510', '520')\r AND\tAMA.CLIENTID IS NOT NULL\r AND   \tTO_DATE('$$MP_BUSDATE','YYYYMMDD') BETWEEN PLB.EFROM \r                               AND NVL(PLB.ETO, TO_DATE('40001231', 'YYYYMMDD'))\r AND   \tTO_DATE('$$MP_BUSDATE','YYYYMMDD') BETWEEN PLI.EFROM \r                               AND NVL(PLI.ETO, TO_DATE('40001231', 'YYYYMMDD'))"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/m_mmrt_credit_card_plan_cal_hist_sum_load/{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_credit_card_plan_cal_hist_sum_load_SQ_CREDIT_PLAN.yml"
          )
        ),
        input_ports = None,
        is_custom_output_schema = True
    )
    (
        _11_0_1__database___generate_monthly_financialsm_mmrt_credit_card_plan_cal_hist_sum_load_sq_credit_plan
        >> m_mmrt_credit_card_plan_cal_hist_sum_load__sm_credit_card_plan_cal_mth_his
    )
