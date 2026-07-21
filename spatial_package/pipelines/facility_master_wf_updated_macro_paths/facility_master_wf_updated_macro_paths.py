from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "facility_master_wf_updated_macro_paths",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'facility_master_wf_updated_macro_paths'")
)

with Pipeline(args) as pipeline:
    batchmacrooutputboundarytruncate_macro_1004 = Process(
        name = "BatchMacroOutputBoundaryTruncate_Macro_1004",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "spark.sql(\"TRUNCATE TABLE table_1004_Output6_macro_op\")\nout0 = in0"
        ),
        is_custom_output_schema = True
    )
    batchmacrooutputboundarytruncate_macro_1070 = Process(
        name = "BatchMacroOutputBoundaryTruncate_Macro_1070",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "spark.sql(\"TRUNCATE TABLE table_1070_Output17_macro_op\")\nout0 = in0"
        ),
        is_custom_output_schema = True
    )
    join_946 = Process(
        name = "Join_946",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "\n#Failed to parse Join component, error: Invalid input ports / connections found\n#<Node ToolID=\"946\">\n#              <GuiSettings Plugin=\"AlteryxBasePluginsGui.Join.Join\">\n#                <Position y=\"4074\" x=\"7690\"/>\n#              </GuiSettings>\n#              <Properties>\n#                <Configuration joinByRecordPos=\"False\">\n#                  <JoinInfo connection=\"Left\">\n#                    <Field field=\"GroupID\"/>\n#                    <Field field=\"city_town_village\"/>\n#                  </JoinInfo>\n#                  <JoinInfo connection=\"Right\">\n#                    <Field field=\"GroupID\"/>\n#                    <Field field=\"city_town_village\"/>\n#                  </JoinInfo>\n#                  <SelectConfiguration>\n#                    <Configuration outputConnection=\"Join\">\n#                      <OrderChanged value=\"False\"/>\n#                      <CommaDecimal value=\"False\"/>\n#                      <SelectFields>\n#                        <SelectField input=\"Left_\" selected=\"True\" field=\"Left_city_town_village\"/>\n#                        <SelectField input=\"Left_\" selected=\"True\" field=\"Left_lat\"/>\n#                        <SelectField input=\"Left_\" selected=\"True\" field=\"Left_lot\"/>\n#                        <SelectField input=\"Left_\" selected=\"True\" field=\"Left_latitude\"/>\n#                        <SelectField input=\"Left_\" selected=\"True\" field=\"Left_longitude\"/>\n#                        <SelectField input=\"Left_\" selected=\"True\" field=\"Left_new_org_name_alteryx\"/>\n#                        <SelectField input=\"Left_\" selected=\"True\" field=\"Left_organization_name\"/>\n#                        <SelectField input=\"Left_\" selected=\"True\" field=\"Left_GroupID\"/>\n#                        <SelectField size=\"65530\" input=\"Right_\" rename=\"new\" selected=\"True\" field=\"Right_new_org_name_alteryx\"/>\n#                        <SelectField input=\"Right_\" rename=\"Right_lat\" selected=\"False\" field=\"Right_lat\"/>\n#                        <SelectField input=\"Right_\" rename=\"Right_lot\" selected=\"False\" field=\"Right_lot\"/>\n#                        <SelectField selected=\"False\" field=\"*Unknown\"/>\n#                      </SelectFields>\n#                    </Configuration>\n#                  </SelectConfiguration>\n#                </Configuration>\n#                <Annotation DisplayMode=\"0\">\n#                  <Name/>\n#                  <DefaultAnnotationText/>\n#                  <Left value=\"False\"/>\n#                </Annotation>\n#              </Properties>\n#              <EngineSettings EngineDllEntryPoint=\"AlteryxJoin\" EngineDll=\"AlteryxBasePluginsEngine.dll\"/>\n#            </Node>\n"
        ),
        output_ports = 2,
        is_custom_output_schema = True
    )
    macro_1004 = Process(
        name = "Macro_1004",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          iteratorMode = False,
          enableMaxTriggers = False,
          pipelineName = "facility_master_wf_updated_macro_paths_1004",
          parameters = {"Control_Parameter" : "EXP"}
        ),
        is_custom_output_schema = True
    )
    macro_1070 = Process(
        name = "Macro_1070",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          iteratorMode = False,
          enableMaxTriggers = False,
          pipelineName = "facility_master_wf_updated_macro_paths_1070",
          parameters = {"GROUP" : "GroupID"}
        ),
        is_custom_output_schema = True
    )
    cb_2018_us_coun_204 = Process(
        name = "cb_2018_us_coun_204",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Desktop\\pfizer\\OARS US Ken Kuang\\facility_master_sources\\cb_2018_us_county_5m.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/facility_master_wf_updated_macro_paths/cb_2018_us_coun_204.yml"
          )
        ),
        input_ports = None
    )
    cb_2018_us_stat_207 = Process(
        name = "cb_2018_us_stat_207",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Desktop\\pfizer\\OARS US Ken Kuang\\facility_master_sources\\cb_2018_us_state_5m.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/facility_master_wf_updated_macro_paths/cb_2018_us_stat_207.yml"
          )
        ),
        input_ports = None
    )
    contact_address_1053 = Process(
        name = "contact_address_1053",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Desktop\\pfizer\\OARS US Ken Kuang\\facility_master_sources\\hblair-facility_master_wf\\contact_address.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/facility_master_wf_updated_macro_paths/contact_address_1053.yml"
          )
        ),
        input_ports = None
    )
    contact_informa_2 = Process(
        name = "contact_informa_2",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Desktop\\pfizer\\OARS US Ken Kuang\\facility_master_sources\\contact_information.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/facility_master_wf_updated_macro_paths/contact_informa_2.yml"
          )
        ),
        input_ports = None
    )
    contact_org_csv_24 = Process(
        name = "contact_org_csv_24",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Desktop\\pfizer\\OARS US Ken Kuang\\facility_master_sources\\contact_org.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/facility_master_wf_updated_macro_paths/contact_org_csv_24.yml"
          )
        ),
        input_ports = None
    )
    contact_person__28 = Process(
        name = "contact_person__28",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Desktop\\pfizer\\OARS US Ken Kuang\\facility_master_sources\\contact_person.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/facility_master_wf_updated_macro_paths/contact_person__28.yml"
          )
        ),
        input_ports = None
    )
    continuity_geoc_1056 = Process(
        name = "continuity_geoc_1056",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Desktop\\pfizer\\OARS US Ken Kuang\\facility_master_sources\\hblair-facility_master_wf\\continuity_geocoded_clinical_sites_posscs.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/facility_master_wf_updated_macro_paths/continuity_geoc_1056.yml"
          )
        ),
        input_ports = None
    )
    facility_master_wf_updated_macro_paths__filter_221 = Process(
        name = "facility_master_wf_updated_macro_paths__Filter_221",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Filter_221")
    )
    facility_master_wf_updated_macro_paths__filter_867 = Process(
        name = "facility_master_wf_updated_macro_paths__Filter_867",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Filter_867")
    )
    facility_master_wf_updated_macro_paths__formula_793_to_formula_1078_0 = Process(
        name = "facility_master_wf_updated_macro_paths__Formula_793_to_Formula_1078_0",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Formula_793_to_Formula_1078_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    facility_master_wf_updated_macro_paths__join_863_inner = Process(
        name = "facility_master_wf_updated_macro_paths__Join_863_inner",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Join_863_inner"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    facility_master_wf_updated_macro_paths__multifieldformula_1035 = Process(
        name = "facility_master_wf_updated_macro_paths__MultiFieldFormula_1035",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__MultiFieldFormula_1035"),
        input_ports = None
    )
    facility_master_wf_updated_macro_paths__multifieldformula_1037 = Process(
        name = "facility_master_wf_updated_macro_paths__MultiFieldFormula_1037",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__MultiFieldFormula_1037"),
        input_ports = None
    )
    facility_master_wf_updated_macro_paths__multifieldformula_399 = Process(
        name = "facility_master_wf_updated_macro_paths__MultiFieldFormula_399",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__MultiFieldFormula_399"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    facility_master_wf_updated_macro_paths__multifieldformula_528 = Process(
        name = "facility_master_wf_updated_macro_paths__MultiFieldFormula_528",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__MultiFieldFormula_528"),
        input_ports = None
    )
    facility_master_wf_updated_macro_paths__summarize_1020 = Process(
        name = "facility_master_wf_updated_macro_paths__Summarize_1020",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Summarize_1020")
    )
    facility_master_wf_updated_macro_paths__summarize_239 = Process(
        name = "facility_master_wf_updated_macro_paths__Summarize_239",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Summarize_239")
    )
    facility_master_wf_updated_macro_paths__summarize_866 = Process(
        name = "facility_master_wf_updated_macro_paths__Summarize_866",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Summarize_866")
    )
    facility_master_wf_updated_macro_paths__union_897 = Process(
        name = "facility_master_wf_updated_macro_paths__Union_897",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Union_897"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    facility_master_wf_updated_macro_paths__union_947 = Process(
        name = "facility_master_wf_updated_macro_paths__Union_947",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Union_947"),
        input_ports = ["in_0", "in_1"]
    )
    facility_master_wf_updated_macro_paths__unique_1018 = Process(
        name = "facility_master_wf_updated_macro_paths__Unique_1018",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Unique_1018")
    )
    facility_master_wf_updated_macro_paths__unique_949 = Process(
        name = "facility_master_wf_updated_macro_paths__Unique_949",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__Unique_949"),
        input_ports = None
    )
    facility_master_wf_updated_macro_paths__aka_gpd_uddl_wr_788 = Process(
        name = "facility_master_wf_updated_macro_paths__aka_GPD_UDDL_Wr_788",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__aka_GPD_UDDL_Wr_788")
    )
    facility_master_wf_updated_macro_paths__table_1004_input43_macro_ip = Process(
        name = "facility_master_wf_updated_macro_paths__table_1004_Input43_macro_ip",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__table_1004_Input43_macro_ip")
    )
    facility_master_wf_updated_macro_paths__table_1070_input12_macro_ip = Process(
        name = "facility_master_wf_updated_macro_paths__table_1070_Input12_macro_ip",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths__table_1070_Input12_macro_ip")
    )
    site_studysite__412 = Process(
        name = "site_studysite__412",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\prophecy\\Desktop\\pfizer\\OARS US Ken Kuang\\facility_master_sources\\site_studysite_org_base_kuangc07.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/facility_master_wf_updated_macro_paths/site_studysite__412.yml"
          )
        ),
        input_ports = None
    )
    facility_master_wf_updated_macro_paths__unique_949 >> join_946
    facility_master_wf_updated_macro_paths__table_1070_input12_macro_ip >> batchmacrooutputboundarytruncate_macro_1070
    (
        facility_master_wf_updated_macro_paths__summarize_1020._out(0)
        >> [facility_master_wf_updated_macro_paths__unique_1018._in(0),
              facility_master_wf_updated_macro_paths__formula_793_to_formula_1078_0._in(2)]
    )
    (
        facility_master_wf_updated_macro_paths__summarize_239._out(0)
        >> [facility_master_wf_updated_macro_paths__formula_793_to_formula_1078_0._in(1),
              facility_master_wf_updated_macro_paths__join_863_inner._in(1)]
    )
    facility_master_wf_updated_macro_paths__filter_867 >> macro_1004
    (
        facility_master_wf_updated_macro_paths__join_863_inner._out(0)
        >> [facility_master_wf_updated_macro_paths__union_897._in(1),
              facility_master_wf_updated_macro_paths__summarize_866._in(0),
              facility_master_wf_updated_macro_paths__table_1004_input43_macro_ip._in(0)]
    )
    (
        facility_master_wf_updated_macro_paths__summarize_866._out(0)
        >> [facility_master_wf_updated_macro_paths__filter_867._in(0),
              facility_master_wf_updated_macro_paths__union_897._in(0)]
    )
    contact_address_1053 >> facility_master_wf_updated_macro_paths__multifieldformula_399._in(4)
    cb_2018_us_coun_204 >> facility_master_wf_updated_macro_paths__multifieldformula_399._in(2)
    continuity_geoc_1056 >> facility_master_wf_updated_macro_paths__multifieldformula_399._in(5)
    (
        facility_master_wf_updated_macro_paths__multifieldformula_399._out(0)
        >> [facility_master_wf_updated_macro_paths__join_863_inner._in(0),
              facility_master_wf_updated_macro_paths__filter_221._in(0),
              facility_master_wf_updated_macro_paths__aka_gpd_uddl_wr_788._in(0)]
    )
    contact_informa_2 >> facility_master_wf_updated_macro_paths__multifieldformula_399._in(6)
    cb_2018_us_stat_207 >> facility_master_wf_updated_macro_paths__multifieldformula_399._in(3)
    facility_master_wf_updated_macro_paths__unique_1018 >> macro_1070
    join_946._out(1) >> facility_master_wf_updated_macro_paths__union_947._in(1)
    (
        facility_master_wf_updated_macro_paths__filter_221._out(0)
        >> [facility_master_wf_updated_macro_paths__formula_793_to_formula_1078_0._in(3),
              facility_master_wf_updated_macro_paths__join_863_inner._in(2),
              facility_master_wf_updated_macro_paths__summarize_239._in(0)]
    )
    site_studysite__412 >> facility_master_wf_updated_macro_paths__formula_793_to_formula_1078_0._in(0)
    contact_person__28 >> facility_master_wf_updated_macro_paths__multifieldformula_399._in(1)
    (
        facility_master_wf_updated_macro_paths__union_897._out(0)
        >> [facility_master_wf_updated_macro_paths__formula_793_to_formula_1078_0._in(4),
              facility_master_wf_updated_macro_paths__summarize_1020._in(0),
              facility_master_wf_updated_macro_paths__table_1070_input12_macro_ip._in(0)]
    )
    facility_master_wf_updated_macro_paths__table_1004_input43_macro_ip >> batchmacrooutputboundarytruncate_macro_1004
    join_946._out(0) >> facility_master_wf_updated_macro_paths__union_947._in(0)
    contact_org_csv_24 >> facility_master_wf_updated_macro_paths__multifieldformula_399._in(0)
