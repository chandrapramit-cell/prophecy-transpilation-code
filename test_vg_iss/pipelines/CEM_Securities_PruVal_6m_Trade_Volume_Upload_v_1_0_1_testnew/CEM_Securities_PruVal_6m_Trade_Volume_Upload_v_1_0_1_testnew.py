from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew",
    version = 1,
    auto_layout = False,
    params = Parameters(
      Select_the_LOB_ = "''",
      variable18_Disabled_value = "CASE WHEN (Select_the_LOB_ = 'Rates') THEN 'False' ELSE 'True' END",
      variable46_Disabled_value = "CASE WHEN (Select_the_LOB_ = 'Credits DM') THEN 'False' ELSE 'True' END",
      variable74_Disabled_value = "CASE WHEN (Select_the_LOB_ = 'Credits EM') THEN 'False' ELSE 'True' END",
      variable143_Disabled_value = "CASE WHEN (Select_the_LOB_ = 'CEM') THEN 'False' ELSE 'True' END",
      workflow_name = "'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew'",
      Question__Drop_Down_172 = "''"
    )
)

with Pipeline(args) as pipeline:
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_142 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_142",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_142"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_21 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_21",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_21"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_226 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_226",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_226")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_38 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_38",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_38"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_4 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_4",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_4")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_52 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_52",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_52")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_65 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_65",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_65"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_77 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_77",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_77")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_96 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_96",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_96")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__filter_206 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Filter_206",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Filter_206")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__filter_93 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Filter_93",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Filter_93")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_198_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_198_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_198_0")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_199_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_199_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_199_0")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_200_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_200_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_200_0"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_208_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_208_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_208_0"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_213_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_213_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_213_0"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_218_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_218_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_218_0"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_229_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_229_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_229_0")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_50_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_50_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_50_0"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_82_0 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_82_0",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_82_0")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__join_94_left = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Join_94_left",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Join_94_left"),
        input_ports = ["in_0", "in_1"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_124 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Summarize_124",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Summarize_124")
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_16 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Summarize_16",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Summarize_16"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_231 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Summarize_231",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Summarize_231"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_103 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Union_103",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Union_103"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_197 = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Union_197",
        properties = ModelTransform(modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Union_197"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    configuration_t_137 = Process(
        name = "Configuration_t_137",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\CEM\\Configuration_template_CEM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_137.yml",
            sheetName = "Product Mapping",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_14 = Process(
        name = "Configuration_t_14",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\Rates\\Configuration_template_rates.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_14.yml",
            sheetName = "Mapping",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_28 = Process(
        name = "Configuration_t_28",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\Credits_DM\\Configuration_template_Credit_DM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_28.yml",
            sheetName = "Mapping",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_29 = Process(
        name = "Configuration_t_29",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\Credits_DM\\Configuration_template_Credit_DM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_29.yml",
            sheetName = "Weights",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_5 = Process(
        name = "Configuration_t_5",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\Rates\\Configuration_template_rates.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_5.yml",
            sheetName = "Weights",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
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
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_57.yml",
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
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_58.yml",
            sheetName = "Weights",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_83 = Process(
        name = "Configuration_t_83",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\CEM\\Configuration_template_CEM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_83.yml",
            sheetName = "Weights",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_88 = Process(
        name = "Configuration_t_88",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\CEM\\Configuration_template_CEM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_88.yml",
            sheetName = "Instrument Mapping",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_89 = Process(
        name = "Configuration_t_89",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\CEM\\Configuration_template_CEM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_89.yml",
            sheetName = "FX Rates",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    configuration_t_90 = Process(
        name = "Configuration_t_90",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\CEM\\Configuration_template_CEM.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/Configuration_t_90.yml",
            sheetName = "Counterparty Internal",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    directory_1 = Process(
        name = "Directory_1",
        properties = Directory(
          path = "..\\1.Inputs\\Rates",
          pattern = "*.zip",
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
    directory_78 = Process(
        name = "Directory_78",
        properties = Directory(
          path = "..\\1.Inputs\\CEM",
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
    macro_75 = Process(
        name = "Macro_75",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          iteratorMode = False,
          enableMaxTriggers = False,
          pipelineName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew_75",
          parameters = {"Zip_load_path" : "FullPath"}
        ),
        is_custom_output_schema = True
    )
    macro_76 = Process(
        name = "Macro_76",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          iteratorMode = False,
          enableMaxTriggers = False,
          pipelineName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew_76",
          parameters = {"CSV_load_path" : "FullPath"}
        ),
        is_custom_output_schema = True
    )
    macro_79 = Process(
        name = "Macro_79",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          iteratorMode = False,
          enableMaxTriggers = False,
          pipelineName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew_79",
          parameters = {"CSV_load_path" : "FullPath"}
        ),
        is_custom_output_schema = True
    )
    nan_credit_dm_x_26 = Process(
        name = "NaN_Credit_DM_x_26",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "..\\1.Inputs\\Credits_DM\\NaN_Credit_DM.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation()
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew/NaN_Credit_DM_x_26.yml",
            sheetName = "Credit_DM",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    portfoliocomposerrender_144 = Process(
        name = "PortfolioComposerRender_144",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposerrender_187 = Process(
        name = "PortfolioComposerRender_187",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposerrender_195 = Process(
        name = "PortfolioComposerRender_195",
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
    portfoliocomposerrender_216 = Process(
        name = "PortfolioComposerRender_216",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposerrender_220 = Process(
        name = "PortfolioComposerRender_220",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposerrender_24 = Process(
        name = "PortfolioComposerRender_24",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposerrender_45 = Process(
        name = "PortfolioComposerRender_45",
        properties = Visualize(),
        output_ports = None,
        is_custom_output_schema = True
    )
    portfoliocomposertable_146 = Process(
        name = "PortfolioComposerTable_146",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_192 = Process(
        name = "PortfolioComposerTable_192",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_193 = Process(
        name = "PortfolioComposerTable_193",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_194 = Process(
        name = "PortfolioComposerTable_194",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_209 = Process(
        name = "PortfolioComposerTable_209",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_214 = Process(
        name = "PortfolioComposerTable_214",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_219 = Process(
        name = "PortfolioComposerTable_219",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_22 = Process(
        name = "PortfolioComposerTable_22",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_230 = Process(
        name = "PortfolioComposerTable_230",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_39 = Process(
        name = "PortfolioComposerTable_39",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    portfoliocomposertable_66 = Process(
        name = "PortfolioComposerTable_66",
        properties = Visualize(),
        is_custom_output_schema = True
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_229_0 >> portfoliocomposertable_230
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_38 >> portfoliocomposertable_39
    directory_51 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_52
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_65 >> portfoliocomposertable_66
    portfoliocomposertable_219 >> portfoliocomposerrender_220
    directory_78 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_77
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__join_94_left._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_103._in(4),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_198_0._in(0)]
    )
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_82_0._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_103._in(3),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__filter_93._in(0)]
    )
    configuration_t_90 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_103._in(0)
    configuration_t_88 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_96
    (
        configuration_t_29._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_50_0._in(1),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_50_0._in(2)]
    )
    portfoliocomposertable_214 >> portfoliocomposerrender_216
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_103._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_199_0._in(0),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_142._in(3),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__filter_206._in(0)]
    )
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_124._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_200_0._in(0),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_142._in(4)]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_218_0 >> portfoliocomposertable_219
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_50_0._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_213_0._in(1),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_38._in(1)]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_197 >> portfoliocomposerrender_195
    portfoliocomposertable_146 >> portfoliocomposerrender_144
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_199_0 >> portfoliocomposertable_193
    portfoliocomposertable_193 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_197._in(1)
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_4 >> macro_75
    (
        configuration_t_58._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_231._in(0),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_231._in(1)]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_200_0 >> portfoliocomposertable_194
    portfoliocomposertable_66 >> portfoliocomposerrender_187
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_226._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_124._in(0),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_229_0._in(0)]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_77 >> macro_79
    portfoliocomposertable_194 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_197._in(0)
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__filter_93._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_103._in(2),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__join_94_left._in(1)]
    )
    (
        configuration_t_14._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_218_0._in(1),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_21._in(1)]
    )
    portfoliocomposertable_192 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_197._in(2)
    portfoliocomposertable_209 >> portfoliocomposerrender_211
    directory_1 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_4
    nan_credit_dm_x_26 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_50_0._in(0)
    (
        configuration_t_57._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_208_0._in(0),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_65._in(0)]
    )
    (
        configuration_t_83._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_142._in(1),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_142._in(2)]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_52 >> macro_76
    portfoliocomposertable_39 >> portfoliocomposerrender_45
    (
        configuration_t_28._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_213_0._in(0),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_38._in(0)]
    )
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_16._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_218_0._in(0),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_21._in(0)]
    )
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_208_0 >> portfoliocomposertable_209
    (
        configuration_t_5._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_16._in(0),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_16._in(1)]
    )
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__summarize_231._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_208_0._in(1),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_65._in(1)]
    )
    configuration_t_89 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_226
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_213_0 >> portfoliocomposertable_214
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_21 >> portfoliocomposertable_22
    portfoliocomposertable_230 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_197._in(3)
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__filter_206._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_200_0._in(1),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_142._in(5)]
    )
    (
        cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_96._out(0)
        >> [cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__union_103._in(1),
              cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__join_94_left._in(0)]
    )
    configuration_t_137 >> cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_142._in(0)
    portfoliocomposertable_22 >> portfoliocomposerrender_24
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__alteryxselect_142 >> portfoliocomposertable_146
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1_testnew__formula_198_0 >> portfoliocomposertable_192
