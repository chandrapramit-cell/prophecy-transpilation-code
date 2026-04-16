from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_mmrt_drms_cal_hist_sum_load",
    version = 1,
    auto_layout = False,
    params = Parameters(MP_BUSDATE = "''", username = "''", SCHEMA = "'SCHEMA'", jdbcUrl = "''", password = "''")
)

with Pipeline(args) as pipeline:
    m_mmrt_drms_cal_hist_sum_load__sm_drms_cal_mth_end_hist = Process(
        name = "m_mmrt_drms_cal_hist_sum_load__SM_DRMS_CAL_MTH_END_HIST",
        properties = ModelTransform(modelName = "m_mmrt_drms_cal_hist_sum_load__SM_DRMS_CAL_MTH_END_HIST")
    )
    _11_0_1__database___generate_monthly_financialsm_mmrt_drms_cal_hist_sum_load_sq_drms_detail = Process(
        name = "{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_drms_cal_hist_sum_load_SQ_DRMS_DETAIL",
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
              query = "SELECT /*+ parallel(DTL) parallel(CAL) parallel(AMA) use_hash(DTL CAL AMA) */\r SUBSTR('$$MP_BUSDATE',1,6) PERIODID,\r DTL.ACCOUNT_NUM,\r DTL.ACCOUNT_BALANCE,\r DTL.ACCOUNT_STATUS,\r DTL.ASSOCIATED_COSTS,\r DTL.JUDGEMENT_ARREARS,\r DTL.JUDGEMENT_SOLICITOR_COSTS,\r DTL.LAST_PAYMENT_DATE,\r DTL.ORDER_FEE,\r DTL.PAID_SINCE_JUDGEMENT,\r DTL.PAYMENT_AMOUNT,\r DTL.PAYMENT_DUE_AMOUNT,\r DTL.RECOVERY_COSTS,\r DTL.RECOVERY_INTEREST,\r DTL.RECOVERY_PRINCIPAL,\r DTL.SUMMONS_AMOUNT,\r DTL.SUMMONS_FEE,\r DTL.SUMMONS_SOLICITOR_COSTS,\r DTL.WARRANT_COSTS,\r DTL.EFROM,\r DTL.LOAN_TYPE_CODE,\r DTL.CHARGE_OFF_AMOUNT,\r DTL.CHARGE_OFF_DATE\r FROM   DRMS_DETAIL           DTL,\r        ALL_MONTHLY_ACCOUNTS  AMA\r WHERE  TO_DATE('$$MP_BUSDATE','YYYYMMDD')  BETWEEN DTL.EFROM AND NVL(DTL.ETO, TO_DATE('40001231', 'YYYYMMDD'))\r AND    AMA.ACCID = DTL.ACCID"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/m_mmrt_drms_cal_hist_sum_load/{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_drms_cal_hist_sum_load_SQ_DRMS_DETAIL.yml"
          )
        ),
        input_ports = None,
        is_custom_output_schema = True
    )
    (
        _11_0_1__database___generate_monthly_financialsm_mmrt_drms_cal_hist_sum_load_sq_drms_detail
        >> m_mmrt_drms_cal_hist_sum_load__sm_drms_cal_mth_end_hist
    )
