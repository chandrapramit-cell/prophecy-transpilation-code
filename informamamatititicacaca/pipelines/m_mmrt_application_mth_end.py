from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_mmrt_application_mth_end",
    version = 1,
    auto_layout = False,
    params = Parameters(MP_BUSDATE = "''", username = "''", SCHEMA = "'SCHEMA'", jdbcUrl = "''", password = "''")
)

with Pipeline(args) as pipeline:
    m_mmrt_application_mth_end__sm_application_cal_mth_end = Process(
        name = "m_mmrt_application_mth_end__SM_APPLICATION_CAL_MTH_END",
        properties = ModelTransform(modelName = "m_mmrt_application_mth_end__SM_APPLICATION_CAL_MTH_END")
    )
    _11_0_1__database___generate_monthly_financialsm_mmrt_application_mth_end_sqtrans = Process(
        name = "{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_application_mth_end_SQTRANS",
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
              query = "SELECT /*+ parallel(AMA) parallel(APP) \r        use_hash(AMA APP) */\r        TO_NUMBER(SUBSTR('$$MP_BUSDATE',1,6)) PERIODID,\r        APP.APPLICATION_DATE,\r        APP.ACCID,\r        APP.APPLICATION_TYPE,\r        APP.CARD_PROTECTION,\r        APP.CARDSAFE,\r        APP.CUSTOMER_NUMBER,\r        APP.DISPLAY_ACCOUNT_NUMBER,\r        APP.EMBOSSER_NUMBER_AU,\r        AMA.ACC_NUM,\r        APP.FINAL_DECISION_COARSE,\r        AMA.STATUS,\r        APP.INITIAL_DECISION_COARSE,\r        APP.MIGRATION_FLAG,\r        APP.OFFER_CODE,\r        AMA.ACC_TYPE,\r        APP.PRODUCT_CODE,\r        AMA.SOURCE_CODE,\r        AMA.CLIENTID,\r        APP.ACTION_DATE,\r        APP.FINAL_DECISION_PRIMARY,\r        APP.SYSTEM_CREDIT_LIMIT,\r        APP.CARD_ORIGIN\r FROM   ALL_MONTHLY_ACCOUNTS  AMA, \r        CREDIT_APPLICATION APP\r WHERE  AMA.ACCID              = APP.ACCID\r AND    AMA.SOURCE_SYSTEM_CODE IN ('M', 'U')\r AND    AMA.CLIENTID           IS NOT NULL\r   AND\tAPP.EFROM > \t( SELECT to_date(CURRVALUE,'YYYYMMDD') FROM DBATTRIBUTE\r \t                  WHERE NAME = 'lastmartrundate' )\r   AND\tTO_DATE('$$MP_BUSDATE','YYYYMMDD') BETWEEN APP.EFROM AND NVL(APP.ETO,TO_DATE('40000101','YYYYMMDD'))"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/m_mmrt_application_mth_end/{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_application_mth_end_SQTRANS.yml"
          )
        ),
        input_ports = None,
        is_custom_output_schema = True
    )
    (
        _11_0_1__database___generate_monthly_financialsm_mmrt_application_mth_end_sqtrans
        >> m_mmrt_application_mth_end__sm_application_cal_mth_end
    )
