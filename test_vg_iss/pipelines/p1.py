from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    configuration_t_57 = Process(
        name = "Configuration_t_57",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\Credits_EM\\Configuration_template_Credit_EM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/p1/Configuration_t_57.yml",
            sheetName = "Mapping",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_58 = Process(
        name = "Configuration_t_58",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\Credits_EM\\Configuration_template_Credit_EM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/p1/Configuration_t_58.yml",
            sheetName = "Weights",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    directory_51 = Process(
        name = "Directory_51",
        properties = Directory(
          path = "..\\1.Inputs\\Credits_EM",
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
    macro_76 = Process(
        name = "Macro_76",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          iteratorMode = False,
          enableMaxTriggers = False,
          pipelineName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1__76",
          parameters = {"CSV_load_path" : "FullPath"}
        ),
        is_custom_output_schema = True
    )
    portfoliocomposerrender_187 = Process(
        name = "PortfolioComposerRender_187",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposerrender_211 = Process(
        name = "PortfolioComposerRender_211",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposertable_209 = Process(
        name = "PortfolioComposerTable_209",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_66 = Process(
        name = "PortfolioComposerTable_66",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    p1__alteryxselect_52 = Process(
        name = "p1__AlteryxSelect_52",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_52")
    )
    p1__alteryxselect_65 = Process(
        name = "p1__AlteryxSelect_65",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_65"),
        input_ports = ["in_0", "in_1"]
    )
    p1__formula_208_0 = Process(
        name = "p1__Formula_208_0",
        properties = ModelTransform(modelName = "p1__Formula_208_0"),
        input_ports = ["in_0", "in_1"]
    )
    p1__summarize_231 = Process(
        name = "p1__Summarize_231",
        properties = ModelTransform(modelName = "p1__Summarize_231"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    p1__summarize_231._out(0) >> [p1__formula_208_0._in(1), p1__alteryxselect_65._in(1)]
    p1__alteryxselect_65 >> portfoliocomposertable_66
    p1__formula_208_0 >> portfoliocomposertable_209
    portfoliocomposertable_66 >> portfoliocomposerrender_187
    portfoliocomposertable_209 >> portfoliocomposerrender_211
    configuration_t_57._out(0) >> [p1__formula_208_0._in(0), p1__alteryxselect_65._in(0)]
    p1__alteryxselect_52 >> macro_76
    directory_51 >> p1__alteryxselect_52
    configuration_t_58._out(0) >> [p1__summarize_231._in(0), p1__summarize_231._in(1)]
