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
    m_mmrt_month_end_table_truncate__sq_dummy = Process(
        name = "m_mmrt_month_end_table_truncate__SQ_DUMMY",
        properties = ModelTransform(modelName = "m_mmrt_month_end_table_truncate__SQ_DUMMY"),
        input_ports = None
    )
    (
        m_mmrt_month_end_table_truncate__sq_dummy._out(0)
        >> [m_mmrt_month_end_table_truncate__sm_credit_card_tran_mth_end._in(0),
              m_mmrt_month_end_table_truncate__sm_storecard_tran_mth_end._in(0)]
    )
