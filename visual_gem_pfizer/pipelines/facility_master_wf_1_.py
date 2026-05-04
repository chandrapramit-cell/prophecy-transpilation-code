from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "facility_master_wf_1_",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_GPDIP_EDLUD_28 = "''",
      password_aka_GPDIP_EDLUD_28 = "''",
      username_aka_GPDIP_EDLUD_24 = "''",
      password_aka_GPDIP_EDLUD_24 = "''",
      username_cb_2018_us_stat_207 = "''",
      password_cb_2018_us_stat_207 = "''",
      username_cb_2018_us_coun_204 = "''",
      password_cb_2018_us_coun_204 = "''",
      username_aka_GPDIP_EDLUD_1053 = "''",
      password_aka_GPDIP_EDLUD_1053 = "''",
      username_aka_GPDIP_EDLUD_1056 = "''",
      password_aka_GPDIP_EDLUD_1056 = "''",
      username_aka_GPDIP_EDLUD_2 = "''",
      password_aka_GPDIP_EDLUD_2 = "''",
      username_aka_GPDIP_EDLUD_412 = "''",
      password_aka_GPDIP_EDLUD_412 = "''",
      jdbcUrl_aka_GPD_UDDL_Wr_788 = "''",
      username_aka_GPD_UDDL_Wr_788 = "''",
      password_aka_GPD_UDDL_Wr_788 = "''",
      workflow_name = "'facility_master_wf_1_'"
    )
)

with Pipeline(args) as pipeline:
    join_946 = Process(
        name = "Join_946",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "\n#Failed to parse Join component, error: Invalid input ports / connections found\n#<Node ToolID=\"946\">\n#              <GuiSettings Plugin=\"AlteryxBasePluginsGui.Join.Join\">\n#                <Position x=\"7690\" y=\"4074\"/>\n#              </GuiSettings>\n#              <Properties>\n#                <Configuration joinByRecordPos=\"False\">\n#                  <JoinInfo connection=\"Left\">\n#                    <Field field=\"GroupID\"/>\n#                    <Field field=\"city_town_village\"/>\n#                  </JoinInfo>\n#                  <JoinInfo connection=\"Right\">\n#                    <Field field=\"GroupID\"/>\n#                    <Field field=\"city_town_village\"/>\n#                  </JoinInfo>\n#                  <SelectConfiguration>\n#                    <Configuration outputConnection=\"Join\">\n#                      <OrderChanged value=\"False\"/>\n#                      <CommaDecimal value=\"False\"/>\n#                      <SelectFields>\n#                        <SelectField field=\"Left_city_town_village\" selected=\"True\" input=\"Left_\"/>\n#                        <SelectField field=\"Left_lat\" selected=\"True\" input=\"Left_\"/>\n#                        <SelectField field=\"Left_lot\" selected=\"True\" input=\"Left_\"/>\n#                        <SelectField field=\"Left_latitude\" selected=\"True\" input=\"Left_\"/>\n#                        <SelectField field=\"Left_longitude\" selected=\"True\" input=\"Left_\"/>\n#                        <SelectField field=\"Left_new_org_name_alteryx\" selected=\"True\" input=\"Left_\"/>\n#                        <SelectField field=\"Left_organization_name\" selected=\"True\" input=\"Left_\"/>\n#                        <SelectField field=\"Left_GroupID\" selected=\"True\" input=\"Left_\"/>\n#                        <SelectField field=\"Right_new_org_name_alteryx\" selected=\"True\" rename=\"new\" input=\"Right_\" size=\"65530\"/>\n#                        <SelectField field=\"Right_lat\" selected=\"False\" rename=\"Right_lat\" input=\"Right_\"/>\n#                        <SelectField field=\"Right_lot\" selected=\"False\" rename=\"Right_lot\" input=\"Right_\"/>\n#                        <SelectField field=\"*Unknown\" selected=\"False\"/>\n#                      </SelectFields>\n#                    </Configuration>\n#                  </SelectConfiguration>\n#                </Configuration>\n#                <Annotation DisplayMode=\"0\">\n#                  <Name/>\n#                  <DefaultAnnotationText/>\n#                  <Left value=\"False\"/>\n#                </Annotation>\n#              </Properties>\n#              <EngineSettings EngineDll=\"AlteryxBasePluginsEngine.dll\" EngineDllEntryPoint=\"AlteryxJoin\"/>\n#            </Node>\n"
        ),
        output_ports = 2,
        is_custom_output_schema = True
    )
    aka_gpdip_edlud_1053 = Process(
        name = "aka_GPDIP_EDLUD_1053",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcda94d51281464f2f8de",
              "username": "${username_aka_GPDIP_EDLUD_1053}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_1053"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select gpdip_prd.gpdip_cdm.contact_address_v.* \nfrom gpdip_prd.gpdip_cdm.contact_address_v\nwhere delete_flag = 'N'"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/facility_master_wf_1_/aka_GPDIP_EDLUD_1053.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_1056 = Process(
        name = "aka_GPDIP_EDLUD_1056",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcda94d51281464f2f8de",
              "username": "${username_aka_GPDIP_EDLUD_1056}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_1056"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT address_id,\n       latitude,\n       longitude\nFROM gpdip_uddl_dna.continuity_geocoded_clinical_sites_posscs"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/facility_master_wf_1_/aka_GPDIP_EDLUD_1056.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_2 = Process(
        name = "aka_GPDIP_EDLUD_2",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcda94d51281464f2f8de",
              "username": "${username_aka_GPDIP_EDLUD_2}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_2"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "Select distinct contact_id,person_id,organization_id,address_id,contact_role,contact_status,primary_contact,study_id,study_site_number From gpdip_prd.gpdip_cdm.contact_information_v\nwhere delete_flag = 'N' and primary_contact = 'Y' and contact_role = 'PRINCIPAL INVESTIGATOR' and contact_status = 'ACTIVE'"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/facility_master_wf_1_/aka_GPDIP_EDLUD_2.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_24 = Process(
        name = "aka_GPDIP_EDLUD_24",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcda94d51281464f2f8de",
              "username": "${username_aka_GPDIP_EDLUD_24}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_24"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select * from gpdip_cdm.contact_organization_v where delete_flag = 'N'"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/facility_master_wf_1_/aka_GPDIP_EDLUD_24.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_28 = Process(
        name = "aka_GPDIP_EDLUD_28",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcda94d51281464f2f8de",
              "username": "${username_aka_GPDIP_EDLUD_28}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_28"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "select * from gpdip_cdm.contact_person_v where delete_flag = 'N'")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/facility_master_wf_1_/aka_GPDIP_EDLUD_28.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_412 = Process(
        name = "aka_GPDIP_EDLUD_412",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcda94d51281464f2f8de",
              "username": "${username_aka_GPDIP_EDLUD_412}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_412"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select * from gpdip_prd.gpdip_uddl_ar.site_studysite_org_base_kuangc07"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/facility_master_wf_1_/aka_GPDIP_EDLUD_412.yml")
        ),
        input_ports = None
    )
    cb_2018_us_coun_204 = Process(
        name = "cb_2018_us_coun_204",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "\\\\SOMNAS500VFS01\\gpdip-prod-fileshare\\ar\\WEATHER_SOURCE_FILES\\GeoSupportFiles\\US_Counties_Census\\cb_2018_us_county_5m\\cb_2018_us_county_5m.shp",
              "username": "${username_cb_2018_us_coun_204}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "cb_2018_us_coun_204"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "\\\\SOMNAS500VFS01\\gpdip-prod-fileshare\\ar\\WEATHER_SOURCE_FILES\\GeoSupportFiles\\US_Counties_Census\\cb_2018_us_county_5m\\cb_2018_us_county_5m.shp"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/facility_master_wf_1_/cb_2018_us_coun_204.yml")
        ),
        input_ports = None
    )
    cb_2018_us_stat_207 = Process(
        name = "cb_2018_us_stat_207",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "\\\\SOMNAS500VFS01\\gpdip-prod-fileshare\\ar\\WEATHER_SOURCE_FILES\\GeoSupportFiles\\US_States_Census\\cb_2018_us_state_5m\\cb_2018_us_state_5m.shp",
              "username": "${username_cb_2018_us_stat_207}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "cb_2018_us_stat_207"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "\\\\SOMNAS500VFS01\\gpdip-prod-fileshare\\ar\\WEATHER_SOURCE_FILES\\GeoSupportFiles\\US_States_Census\\cb_2018_us_state_5m\\cb_2018_us_state_5m.shp"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/facility_master_wf_1_/cb_2018_us_stat_207.yml")
        ),
        input_ports = None
    )
    facility_master_wf_1___formula_793_to_formula_1078_0 = Process(
        name = "facility_master_wf_1___Formula_793_to_Formula_1078_0",
        properties = ModelTransform(modelName = "facility_master_wf_1___Formula_793_to_Formula_1078_0"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    facility_master_wf_1___multifieldformula_1035 = Process(
        name = "facility_master_wf_1___MultiFieldFormula_1035",
        properties = ModelTransform(modelName = "facility_master_wf_1___MultiFieldFormula_1035"),
        input_ports = None
    )
    facility_master_wf_1___multifieldformula_1037 = Process(
        name = "facility_master_wf_1___MultiFieldFormula_1037",
        properties = ModelTransform(modelName = "facility_master_wf_1___MultiFieldFormula_1037"),
        input_ports = None
    )
    facility_master_wf_1___multifieldformula_399 = Process(
        name = "facility_master_wf_1___MultiFieldFormula_399",
        properties = ModelTransform(modelName = "facility_master_wf_1___MultiFieldFormula_399"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    facility_master_wf_1___multifieldformula_528 = Process(
        name = "facility_master_wf_1___MultiFieldFormula_528",
        properties = ModelTransform(modelName = "facility_master_wf_1___MultiFieldFormula_528"),
        input_ports = None
    )
    facility_master_wf_1___union_947 = Process(
        name = "facility_master_wf_1___Union_947",
        properties = ModelTransform(modelName = "facility_master_wf_1___Union_947"),
        input_ports = ["in_0", "in_1"]
    )
    facility_master_wf_1___unique_949 = Process(
        name = "facility_master_wf_1___Unique_949",
        properties = ModelTransform(modelName = "facility_master_wf_1___Unique_949"),
        input_ports = None
    )
    facility_master_wf_1___aka_gpd_uddl_wr_788 = Process(
        name = "facility_master_wf_1___aka_GPD_UDDL_Wr_788",
        properties = ModelTransform(modelName = "facility_master_wf_1___aka_GPD_UDDL_Wr_788")
    )
    facility_master_wf_1___unique_949 >> join_946
    aka_gpdip_edlud_1053 >> facility_master_wf_1___multifieldformula_399._in(3)
    cb_2018_us_coun_204 >> facility_master_wf_1___multifieldformula_399._in(4)
    aka_gpdip_edlud_1056 >> facility_master_wf_1___multifieldformula_399._in(6)
    (
        facility_master_wf_1___multifieldformula_399._out(0)
        >> [facility_master_wf_1___formula_793_to_formula_1078_0._in(1),
              facility_master_wf_1___formula_793_to_formula_1078_0._in(2),
              facility_master_wf_1___aka_gpd_uddl_wr_788._in(0)]
    )
    aka_gpdip_edlud_2 >> facility_master_wf_1___multifieldformula_399._in(5)
    cb_2018_us_stat_207 >> facility_master_wf_1___multifieldformula_399._in(2)
    join_946._out(1) >> facility_master_wf_1___union_947._in(1)
    aka_gpdip_edlud_412 >> facility_master_wf_1___formula_793_to_formula_1078_0._in(0)
    aka_gpdip_edlud_28 >> facility_master_wf_1___multifieldformula_399._in(1)
    join_946._out(0) >> facility_master_wf_1___union_947._in(0)
    aka_gpdip_edlud_24 >> facility_master_wf_1___multifieldformula_399._in(0)
