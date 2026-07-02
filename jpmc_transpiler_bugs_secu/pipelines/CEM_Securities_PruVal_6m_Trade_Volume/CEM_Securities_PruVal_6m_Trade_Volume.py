from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "CEM_Securities_PruVal_6m_Trade_Volume",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'CEM_Securities_PruVal_6m_Trade_Volume'")
)

with Pipeline(args) as pipeline:
    cem_securities_pruval_6m_trade_volume__alteryxselect_31 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__AlteryxSelect_31",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__AlteryxSelect_31")
    )
    cem_securities_pruval_6m_trade_volume__alteryxselect_4 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__AlteryxSelect_4",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__AlteryxSelect_4"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    cem_securities_pruval_6m_trade_volume__alteryxselect_47 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__AlteryxSelect_47",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__AlteryxSelect_47")
    )
    cem_securities_pruval_6m_trade_volume__filter_34 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Filter_34",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Filter_34")
    )
    cem_securities_pruval_6m_trade_volume__filter_50 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Filter_50",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Filter_50")
    )
    cem_securities_pruval_6m_trade_volume__formula_43_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_43_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_43_0")
    )
    cem_securities_pruval_6m_trade_volume__formula_59_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_59_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_59_0"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume__formula_60_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_60_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_60_0")
    )
    cem_securities_pruval_6m_trade_volume__formula_61_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_61_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_61_0")
    )
    cem_securities_pruval_6m_trade_volume__formula_71_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_71_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Formula_71_0")
    )
    cem_securities_pruval_6m_trade_volume__join_33_left = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Join_33_left",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Join_33_left"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume__summarize_13 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Summarize_13",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Summarize_13")
    )
    cem_securities_pruval_6m_trade_volume__union_26 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Union_26",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Union_26"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    cem_securities_pruval_6m_trade_volume__union_62 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume__Union_62",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume__Union_62"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    configuration_t_35 = Process(
        name = "Configuration_t_35",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Documents\\vcg\\Tushar-team-transpiler-project-1\\Securities\\Fake_Data\\Configuration_template_CEM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume/Configuration_t_35.yml",
            sheetName = "Counterparty Internal",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_36 = Process(
        name = "Configuration_t_36",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Documents\\vcg\\Tushar-team-transpiler-project-1\\Securities\\Fake_Data\\Configuration_template_CEM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume/Configuration_t_36.yml",
            sheetName = "FX Rates",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_37 = Process(
        name = "Configuration_t_37",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Documents\\vcg\\Tushar-team-transpiler-project-1\\Securities\\Fake_Data\\Configuration_template_CEM.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation(includeSheetNameColumn = True)
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume/Configuration_t_37.yml",
            sheetName = "Instrument Mapping",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          ),
          compression = DatabricksVolumeSource.Compression(kind = "uncompressed")
        ),
        input_ports = None,
        comment = "Reads instrument mapping (bond descriptions and ISINs) from an Excel file to provide security identifiers for downstream processes."
    )
    configuration_t_38 = Process(
        name = "Configuration_t_38",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Documents\\vcg\\Tushar-team-transpiler-project-1\\Securities\\Fake_Data\\Configuration_template_CEM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume/Configuration_t_38.yml",
            sheetName = "Weights",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_8 = Process(
        name = "Configuration_t_8",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Documents\\vcg\\Tushar-team-transpiler-project-1\\Securities\\Fake_Data\\Configuration_template_CEM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume/Configuration_t_8.yml",
            sheetName = "Product Mapping",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    directory_46 = Process(
        name = "Directory_46",
        properties = Directory(
          path = "C:\\Users\\prophecy\\Documents\\vcg\\Tushar-team-transpiler-project-1\\Securities\\Fake_Data\\CEM",
          pattern = "*.csv",
          integration = "sftp",
          recursive = True,
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {"id" : "transpiled_connection"},
            "type": "connector"
          }
        ),
        input_ports = None
    )
    macro_57 = Process(
        name = "Macro_57",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          iteratorMode = False,
          enableMaxTriggers = False,
          pipelineName = "CEM_Securities_PruVal_6m_Trade_Volume_57",
          parameters = {"CSV_load_path" : "FullPath"}
        ),
        is_custom_output_schema = True
    )
    portfoliocomposerrender_3 = Process(
        name = "PortfolioComposerRender_3",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposerrender_63 = Process(
        name = "PortfolioComposerRender_63",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposertable_1 = Process(
        name = "PortfolioComposerTable_1",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_64 = Process(
        name = "PortfolioComposerTable_64",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_65 = Process(
        name = "PortfolioComposerTable_65",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_66 = Process(
        name = "PortfolioComposerTable_66",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_72 = Process(
        name = "PortfolioComposerTable_72",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    (
        configuration_t_38._out(0)
        >> [cem_securities_pruval_6m_trade_volume__alteryxselect_4._in(1),
              cem_securities_pruval_6m_trade_volume__alteryxselect_4._in(2)]
    )
    portfoliocomposertable_65 >> cem_securities_pruval_6m_trade_volume__union_62._in(1)
    configuration_t_8 >> cem_securities_pruval_6m_trade_volume__alteryxselect_4._in(0)
    cem_securities_pruval_6m_trade_volume__formula_71_0 >> portfoliocomposertable_72
    cem_securities_pruval_6m_trade_volume__formula_59_0 >> portfoliocomposertable_64
    (
        cem_securities_pruval_6m_trade_volume__formula_43_0._out(0)
        >> [cem_securities_pruval_6m_trade_volume__union_26._in(3),
              cem_securities_pruval_6m_trade_volume__filter_34._in(0)]
    )
    (
        cem_securities_pruval_6m_trade_volume__join_33_left._out(0)
        >> [cem_securities_pruval_6m_trade_volume__formula_61_0._in(0),
              cem_securities_pruval_6m_trade_volume__union_26._in(4)]
    )
    portfoliocomposertable_72 >> cem_securities_pruval_6m_trade_volume__union_62._in(3)
    cem_securities_pruval_6m_trade_volume__alteryxselect_4 >> portfoliocomposertable_1
    configuration_t_37 >> cem_securities_pruval_6m_trade_volume__alteryxselect_31
    cem_securities_pruval_6m_trade_volume__alteryxselect_47 >> macro_57
    (
        configuration_t_36._out(0)
        >> [cem_securities_pruval_6m_trade_volume__formula_71_0._in(0),
              cem_securities_pruval_6m_trade_volume__summarize_13._in(0)]
    )
    portfoliocomposertable_66 >> cem_securities_pruval_6m_trade_volume__union_62._in(2)
    directory_46 >> cem_securities_pruval_6m_trade_volume__alteryxselect_47
    cem_securities_pruval_6m_trade_volume__formula_60_0 >> portfoliocomposertable_65
    portfoliocomposertable_1 >> portfoliocomposerrender_3
    (
        cem_securities_pruval_6m_trade_volume__union_26._out(0)
        >> [cem_securities_pruval_6m_trade_volume__formula_60_0._in(0),
              cem_securities_pruval_6m_trade_volume__alteryxselect_4._in(3),
              cem_securities_pruval_6m_trade_volume__filter_50._in(0)]
    )
    (
        cem_securities_pruval_6m_trade_volume__filter_34._out(0)
        >> [cem_securities_pruval_6m_trade_volume__union_26._in(2),
              cem_securities_pruval_6m_trade_volume__join_33_left._in(1)]
    )
    (
        cem_securities_pruval_6m_trade_volume__filter_50._out(0)
        >> [cem_securities_pruval_6m_trade_volume__formula_59_0._in(1),
              cem_securities_pruval_6m_trade_volume__alteryxselect_4._in(5)]
    )
    cem_securities_pruval_6m_trade_volume__formula_61_0 >> portfoliocomposertable_66
    portfoliocomposertable_64 >> cem_securities_pruval_6m_trade_volume__union_62._in(0)
    (
        cem_securities_pruval_6m_trade_volume__summarize_13._out(0)
        >> [cem_securities_pruval_6m_trade_volume__formula_59_0._in(0),
              cem_securities_pruval_6m_trade_volume__alteryxselect_4._in(4)]
    )
    cem_securities_pruval_6m_trade_volume__union_62 >> portfoliocomposerrender_63
    configuration_t_35 >> cem_securities_pruval_6m_trade_volume__union_26._in(0)
    (
        cem_securities_pruval_6m_trade_volume__alteryxselect_31._out(0)
        >> [cem_securities_pruval_6m_trade_volume__union_26._in(1),
              cem_securities_pruval_6m_trade_volume__join_33_left._in(0)]
    )
