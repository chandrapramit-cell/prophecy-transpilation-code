from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_mmrt_monthly_cc_segments_load_sunday",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    mrt_monthly_cc_segment = Process(
        name = "MRT_MONTHLY_CC_SEGMENT",
        properties = Dataset(
          table = Dataset.DBTSource(name = "MRT_MONTHLY_CC_SEGMENT", sourceType = "Table", sourceName = "transpiled_sources"),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    _11_0_1__database___generate_monthly_financialsm_mmrt_monthly_cc_segments_load_sunday_sq_mrt_card_trans_derivations = Process(
        name = "{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_monthly_cc_segments_load_sunday_SQ_MRT_CARD_TRANS_DERIVATIONS",
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
              query = "select /*+ parallel(r1) parallel(mc1) parallel(mc5) use_hash(r1 mc1 mc5) */\r \tmc1.accid,\r case \twhen\r \tmc1.status in ('8','9','Z','F','P','R','T','X')\r \tthen 'Attrition'\r \twhen\r \tmc1.delq_120_days + \r \tmc1.delq_150_days + \r \tmc1.delq_180_days + \r \tmc1.delq_210_days + \r \tmc1.delq_30_days + \r \tmc1.delq_60_days + \r \tmc1.delq_90_days > 0\r \tor\r \tmc1.fraud_type is not null\r \tthen 'Delinquent'\r \twhen\r \tr1.accid is null\r \tthen 'New'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'N'\r \tand\r \tr1.curr_total_int <= 0\r \tand\r \tr1.curr_statement_balance_total < 0\r \tthen\r \t'Inactive Credit Balance'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'N'\r \tand\r \tr1.curr_total_int <= 0\r \tand\r \tr1.curr_statement_balance_total >= 0\r \tand\r \tr1.curr_statement_balance_total <= 1\r \tthen 'Dormant'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'N'\r \tand\r \tr1.curr_total_int <= 0\r \tand\r \tr1.curr_statement_balance_total > 1\r \tthen '0% Offer Paying Down'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'N'\r \tand\r \tr1.curr_total_int > 0\r \tand\r \tr1.curr_statement_balance_total > 1\r \tthen 'Paying Down'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'Y'\r \tand\r \tnvl(mc5.usage_outside_3_months, 'N') = 'Y'\r \tand\r \tr1.curr_val_credits < r1.prev_statement_balance_total\r \tand\r \tr1.prev_statement_balance_total > 1\r \tand\r \tr1.curr_total_int <= 0\r \tthen 'External 0% Offer'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'Y'\r \tand\r \tnvl(mc5.usage_outside_3_months, 'N') = 'Y'\r \tand\r \tr1.curr_val_credits < r1.prev_statement_balance_total\r \tand\r \tr1.prev_statement_balance_total > 1\r \tand\r \tr1.curr_total_int > 0\r \tthen 'External Revolver'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'Y'\r \tand\r \tnvl(mc5.usage_outside_3_months, 'N') = 'Y'\r \tand\r \tr1.curr_val_credits >= r1.prev_statement_balance_total\r \tand\r \tr1.curr_statement_balance_total <= 1\r \tthen 'External Full Payer'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'Y'\r \tand\r \tnvl(mc5.usage_outside_3_months, 'N') = 'Y'\r \tand\r \tr1.curr_val_credits >= r1.prev_statement_balance_total\r \tand\r \tr1.curr_statement_balance_total > 1\r \tthen 'External Full Payer (Current)'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'Y'\r \tand\r \tnvl(mc5.usage_outside_3_months, 'N') = 'N'\r \tand\r \tnvl(mc5.usage_inside_3_months, 'N') = 'Y'\r \tand\r \tr1.curr_val_credits < r1.prev_statement_balance_total\r \tand\r \tr1.curr_statement_balance_total > 1\r \tand\r \tr1.curr_total_int <= 0\r \tthen 'MandS 0% Offer (Not Used)'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'Y'\r \tand\r \tnvl(mc5.usage_outside_3_months, 'N') = 'N'\r \tand\r \tnvl(mc5.usage_inside_3_months, 'N') = 'Y'\r \tand\r \tr1.curr_val_credits < r1.prev_statement_balance_total\r \tand\r \tr1.curr_statement_balance_total > 1\r \tand\r \tr1.curr_total_int > 0\r \tthen 'MandS Revolver'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'Y'\r \tand\r \tnvl(mc5.usage_outside_3_months, 'N') = 'N'\r \tand\r \tnvl(mc5.usage_inside_3_months, 'N') = 'Y'\r \tand\r \tr1.curr_val_credits >= r1.prev_statement_balance_total\r \tand\r \tr1.curr_statement_balance_total <= 1\r \tthen 'MandS Full Payer'\r \twhen\r \tnvl(mc5.scheme_active, 'N') = 'Y'\r \tand\r \tnvl(mc5.usage_outside_3_months, 'N') = 'N'\r \tand\r \tnvl(mc5.usage_inside_3_months, 'N') = 'Y'\r \tand\r \tr1.curr_val_credits >= r1.prev_statement_balance_total\r \tand\r \tr1.curr_statement_balance_total >= 1\t\r \tthen 'MandS Full Payer (Current)'\r \telse 'Unknown'\r end \tsegment\r from\r minervarep.rep_cc_statement_mart_calc r1,\r minervarep.account mc1,\r minervarep.mrt_credit_card_5 mc5\r where\tmc1.accid = r1.accid(+)\r and\tmc1.accid = mc5.accid(+)\r and    mc1.acc_type in ('410','420','510','520')"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/m_mmrt_monthly_cc_segments_load_sunday/{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_monthly_cc_segments_load_sunday_SQ_MRT_CARD_TRANS_DERIVATIONS.yml"
          )
        ),
        input_ports = None,
        is_custom_output_schema = True
    )
    (
        _11_0_1__database___generate_monthly_financialsm_mmrt_monthly_cc_segments_load_sunday_sq_mrt_card_trans_derivations
        >> mrt_monthly_cc_segment
    )
