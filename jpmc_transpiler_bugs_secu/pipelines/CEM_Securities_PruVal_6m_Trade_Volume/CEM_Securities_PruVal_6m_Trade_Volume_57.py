from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "CEM_Securities_PruVal_6m_Trade_Volume_57",
    version = 1,
    auto_layout = False,
    params = Parameters(
      CSV_load_path = "'..\\Inputs\\Credits_EM\\GEM_Credit_Trades_20210801.csv'",
      variable1_File = Expr("{{ var('CSV_load_path') }}"),
      workflow_name = "'CEM_Securities_PruVal_6m_Trade_Volume'",
      Question__ControlParam__Control_Parameter_5 = "''"
    )
)

with Pipeline(args) as pipeline:
    cem_securities_pruval_6m_trade_volume_57__metrics_summary = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_57__metrics_summary",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_57__metrics_summary")
    )
    cem_securities_pruval_6m_trade_volume_57__region_desk_flow_groupby_002 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_57__region_desk_flow_groupBy_002",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_57__region_desk_flow_groupBy_002")
    )
    cem_securities_pruval_6m_trade_volume_57__region_product_heatmap = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_57__region_product_heatmap",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_57__region_product_heatmap")
    )
    cem_securities_pruval_6m_trade_volume_57__scatter_data_projection_002 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_57__scatter_data_projection_002",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_57__scatter_data_projection_002")
    )
    cem_securities_pruval_6m_trade_volume_57__sorted_by_count = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_57__sorted_by_count",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_57__sorted_by_count")
    )
    cem_securities_pruval_6m_trade_volume_57__sorted_by_date = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_57__sorted_by_date",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_57__sorted_by_date")
    )
    cem_securities_pruval_6m_trade_volume_57__trades_by_product_type = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_57__trades_by_product_type",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_57__trades_by_product_type")
    )
    cem_securities_pruval_6m_trade_volume_57__trades_by_region = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_57__trades_by_region",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_57__trades_by_region")
    )
    gem_credit_trad_1_57 = Process(
        name = "GEM_Credit_Trad_1_57",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_57/GEM_Credit_Trad_1_57.yml"
          ),
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\Inputs\\Credits_EM\\GEM_Credit_Trades_20210801.csv"
          )
        ),
        input_ports = None
    )
    table_57_output4_macro_op = Process(
        name = "table_57_Output4_macro_op",
        properties = Dataset(
          table = Dataset.DBTSource(name = "table_57_Output4_macro_op", sourceName = "transpiled_sources", sourceType = "Table"),
          writeOptions = {
            "entityConfig": {
              "database": "\"sony\"",
              "incremental_strategy": "\"append\"",
              "alias": "\"table_57_Output4_macro_op\"",
              "schema": "\"orch_test\"",
              "type": "ModelConfig",
              "materialized": "incremental"
            },
            "writeMode": "append"
          }
        )
    )
    viz_metrics_summary = Process(name = "viz_metrics_summary", properties = Visualize(), output_ports = None)
    viz_region_desk_flow = Process(name = "viz_region_desk_flow", properties = Visualize(), output_ports = None)
    viz_region_product_heatmap = Process(name = "viz_region_product_heatmap", properties = Visualize(), output_ports = None)
    viz_scatter_data = Process(name = "viz_scatter_data", properties = Visualize(), output_ports = None)
    viz_trades_by_date = Process(name = "viz_trades_by_date", properties = Visualize(), output_ports = None)
    viz_trades_by_product_type = Process(name = "viz_trades_by_product_type", properties = Visualize(), output_ports = None)
    viz_trades_by_region = Process(name = "viz_trades_by_region", properties = Visualize(), output_ports = None)
    viz_trades_by_status = Process(name = "viz_trades_by_status", properties = Visualize(), output_ports = None)
    (
        gem_credit_trad_1_57._out(0)
        >> [cem_securities_pruval_6m_trade_volume_57__scatter_data_projection_002._in(0),
              cem_securities_pruval_6m_trade_volume_57__sorted_by_date._in(0),
              cem_securities_pruval_6m_trade_volume_57__trades_by_region._in(0),
              cem_securities_pruval_6m_trade_volume_57__sorted_by_count._in(0),
              cem_securities_pruval_6m_trade_volume_57__metrics_summary._in(0),
              cem_securities_pruval_6m_trade_volume_57__region_product_heatmap._in(0),
              cem_securities_pruval_6m_trade_volume_57__region_desk_flow_groupby_002._in(0),
              cem_securities_pruval_6m_trade_volume_57__trades_by_product_type._in(0)]
    )
    cem_securities_pruval_6m_trade_volume_57__region_desk_flow_groupby_002 >> viz_region_desk_flow
    cem_securities_pruval_6m_trade_volume_57__scatter_data_projection_002 >> viz_scatter_data
    cem_securities_pruval_6m_trade_volume_57__metrics_summary >> viz_metrics_summary
    cem_securities_pruval_6m_trade_volume_57__region_product_heatmap >> viz_region_product_heatmap
    cem_securities_pruval_6m_trade_volume_57__trades_by_product_type >> viz_trades_by_product_type
    cem_securities_pruval_6m_trade_volume_57__trades_by_region >> viz_trades_by_region
    cem_securities_pruval_6m_trade_volume_57__sorted_by_date >> viz_trades_by_date
    cem_securities_pruval_6m_trade_volume_57__sorted_by_count >> viz_trades_by_status
