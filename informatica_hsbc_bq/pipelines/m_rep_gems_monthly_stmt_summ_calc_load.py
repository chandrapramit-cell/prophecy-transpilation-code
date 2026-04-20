from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_gems_monthly_stmt_summ_calc_load",
    version = 1,
    auto_layout = False,
    params = Parameters(MP_BUSDATE = "''", username = "''", SCHEMA = "'SCHEMA'", jdbcUrl = "''", password = "''")
)

with Pipeline(args) as pipeline:
    table_1 = Process(
        name = "Table_1",
        properties = Dataset(
          table = Dataset.DBTSource(name = "edgef", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    m_rep_gems_monthly_stmt_summ_calc_load__exp_identify_correct_periods = Process(
        name = "m_rep_gems_monthly_stmt_summ_calc_load__EXP_IDENTIFY_CORRECT_PERIODS",
        properties = ModelTransform(modelName = "m_rep_gems_monthly_stmt_summ_calc_load__EXP_IDENTIFY_CORRECT_PERIODS"),
        input_ports = None
    )
    m_rep_gems_monthly_stmt_summ_calc_load__reformat_1 = Process(
        name = "m_rep_gems_monthly_stmt_summ_calc_load__Reformat_1",
        properties = ModelTransform(modelName = "m_rep_gems_monthly_stmt_summ_calc_load__Reformat_1")
    )
    table_1 >> m_rep_gems_monthly_stmt_summ_calc_load__reformat_1
