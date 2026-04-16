from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_mmrt_loan_cal_hist_sum_load",
    version = 1,
    auto_layout = False,
    params = Parameters(
      MP_BUSDATE = "''",
      MP_PERIOD_END_DATE = "''",
      username = "''",
      SCHEMA = "'SCHEMA'",
      jdbcUrl = "''",
      password = "''"
    )
)

with Pipeline(args) as pipeline:
    m_mmrt_loan_cal_hist_sum_load__exp_current_delinquency = Process(
        name = "m_mmrt_loan_cal_hist_sum_load__EXP_CURRENT_DELINQUENCY",
        properties = ModelTransform(modelName = "m_mmrt_loan_cal_hist_sum_load__EXP_CURRENT_DELINQUENCY")
    )
    m_mmrt_loan_cal_hist_sum_load__sm_account_good_bad = Process(
        name = "m_mmrt_loan_cal_hist_sum_load__SM_ACCOUNT_GOOD_BAD",
        properties = ModelTransform(modelName = "m_mmrt_loan_cal_hist_sum_load__SM_ACCOUNT_GOOD_BAD")
    )
    m_mmrt_loan_cal_hist_sum_load__sm_loan_cal_mth_end_hist = Process(
        name = "m_mmrt_loan_cal_hist_sum_load__SM_LOAN_CAL_MTH_END_HIST",
        properties = ModelTransform(modelName = "m_mmrt_loan_cal_hist_sum_load__SM_LOAN_CAL_MTH_END_HIST")
    )
    _11_0_1__database___generate_monthly_financialsm_mmrt_loan_cal_hist_sum_load_sqtrans = Process(
        name = "{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_loan_cal_hist_sum_load_SQTRANS",
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
              query = "SELECT /*+ parallel(AMA) parallel(ACH) \r            parallel(BHS) parallel(ACC\r            use_hash(AMA ACH ACC BHS) */\r AMA.ACCID,\r SUBSTR('$$MP_BUSDATE',1,6) PERIODID,\r AMA.ACC_NUM,\r ACH.STATUS,\r --\r NVL(ACH.PAST_DUE,0) + NVL(ACH.DELQ_30_DAYS,0) + NVL(ACH.DELQ_60_DAYS,0) +\r \tNVL(ACH.DELQ_90_DAYS,0) + NVL(ACH.DELQ_120_DAYS,0) + NVL(ACH.DELQ_150_DAYS,0) +\r \tNVL(ACH.DELQ_180_DAYS,0) + NVL(ACH.DELQ_210_DAYS,0) ARREARS_VALUE,\r --\r ACH.BLOCK_CODE,\r ACH.GBP_AMT_CHARGE_OFF,\r ACH.CURR_BALANCE,\r ACH.CYCLE_DAY,\r --\r CASE\tWHEN NVL(ACH.GBP_AMT_CHARGE_OFF,0) > 0 THEN\r     '2'\r \tWHEN NVL(AMA.DATE_CLOSED, TO_DATE('40001231','YYYYMMDD')) < \r \t         ADD_MONTHS(TO_DATE('$$MP_BUSDATE','YYYYMMDD'), -12) THEN\r     '4'\r \tWHEN (NVL(ACH.DELQ_30_DAYS,0) + NVL(ACH.DELQ_60_DAYS,0) +\r \t      NVL(ACH.DELQ_90_DAYS,0) + NVL(ACH.DELQ_120_DAYS,0) + NVL(ACH.DELQ_150_DAYS,0) +\r \t      NVL(ACH.DELQ_180_DAYS,0) + NVL(ACH.DELQ_210_DAYS,0)) > 0 THEN\r     '2'\r \tWHEN ACH.BLOCK_CODE IN ('E','F','L','R','W','X','Y') THEN\r     '2'\r \tWHEN NVL(ACH.PAST_DUE,0) > 0\r          AND ACH.BLOCK_CODE IN ('A','G','K') THEN\r     '2'\r \tWHEN ACH.CONTRA_RESCHEDULE_FLAG = 'R' THEN\r     '2'\r \tWHEN NVL(ACH.PAST_DUE,0) > 0 THEN\r     '3'\r \tWHEN NVL(ACH.CURR_DUE,0) >= 0\r          AND ACH.BLOCK_CODE IN ('A','D','G','I','K') THEN\r     '3'\r \tELSE\r     '1'\r END GOOD_BAD_FLAG,\r --\r ACH.PAST_DUE,\r ACH.PAYMENT_METHOD,\r BHS.SCORE,\r ACH.DELQ_30_DAYS,\r ACH.DELQ_60_DAYS,\r ACH.DELQ_90_DAYS,\r ACH.DELQ_120_DAYS,\r ACH.DELQ_150_DAYS,\r ACH.DELQ_180_DAYS,\r ACH.DELQ_210_DAYS,\r LOA.UNEARNED_INTEREST,\r LOA.REBATE_PROMOTION_INDICATOR,\r ACC.USER_CODE_1,\r ACC.USER_CODE_2\r --\r FROM\tALL_MONTHLY_ACCOUNTS AMA,\r         ACCOUNT ACC,\r \tACCOUNT_HISTORY partition(hot) ACH,\r   \tBEHAVIOUR_SCORE BHS,\r   \tLOAN LOA\r  --\r WHERE AMA.ACCID = ACH.ACCID\r   AND AMA.ACCID = ACC.ACCID\r   AND AMA.ACCID = LOA.ACCID (+)\r   AND AMA.ACCID = BHS.ACCID (+)\r   AND AMA.ACC_TYPE IN ('200','210','220','230','240')\r   AND ACC.ACC_TYPE IN ('200','210','220','230','240')\r   AND AMA.BUSINESS_DATE BETWEEN BHS.EFROM (+) AND NVL(BHS.ETO (+), TO_DATE('40001231','YYYYMMDD'))\r   AND TO_DATE('$$MP_BUSDATE','YYYYMMDD') between ACH.EFROM AND NVL(ACH.ETO,TO_DATE('01014000','DDMMYYYY'))\r   AND AMA.CLIENTID IS NOT NULL\r   AND LOA.EFROM <= TO_DATE('$$MP_PERIOD_END_DATE', 'YYYYMMDD')\r   AND (LOA.ETO IS NULL OR LOA.ETO >= TO_DATE('$$MP_PERIOD_END_DATE', 'YYYYMMDD') )"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/m_mmrt_loan_cal_hist_sum_load/{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_loan_cal_hist_sum_load_SQTRANS.yml"
          )
        ),
        input_ports = None,
        is_custom_output_schema = True
    )
    (
        _11_0_1__database___generate_monthly_financialsm_mmrt_loan_cal_hist_sum_load_sqtrans
        >> m_mmrt_loan_cal_hist_sum_load__exp_current_delinquency
    )
    (
        m_mmrt_loan_cal_hist_sum_load__exp_current_delinquency._out(0)
        >> [m_mmrt_loan_cal_hist_sum_load__sm_loan_cal_mth_end_hist._in(0),
              m_mmrt_loan_cal_hist_sum_load__sm_account_good_bad._in(0)]
    )
