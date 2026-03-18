from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "wf_standard_core_recruitment_review_thaded",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_test_oracle_428 = "''",
      password_aka_test_oracle_563 = "''",
      password_aka_test_oracle_428 = "''",
      username_aka_GPD_UDDL_Wr_366 = "''",
      username_aka_test_oracle_356 = "''",
      workflow_name = "'wf_standard_core_recruitment_review_thaded'",
      password_aka_GPDIP_EDLUD_506 = "''",
      password_aka_test_oracle_490 = "''",
      username_aka_test_oracle_586 = "''",
      password_aka_test_oracle_536 = "''",
      username_aka_GPDIP_EDLUD_506 = "''",
      username_aka_test_oracle_448 = "''",
      username_aka_test_oracle_563 = "''",
      password_aka_GPD_UDDL_Wr_366 = "''",
      password_aka_test_oracle_448 = "''",
      password_aka_test_oracle_356 = "''",
      username_aka_test_oracle_536 = "''",
      jdbcUrl_aka_GPD_UDDL_Wr_366 = "''",
      username_aka_test_oracle_490 = "''",
      password_aka_test_oracle_586 = "''"
    )
)

with Pipeline(args) as pipeline:
    aka_test_oracle_563 = Process(
        name = "aka_test_oracle_563",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:test_oracle",
              "username": "${username_aka_test_oracle_563}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_test_oracle_563"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select * from\n--gpdip_uddl_dna.diversity_control_tower_cohorts_waterfall_pft\nprophecy.diversity_control_tower_cohorts_waterfall_pft"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/wf_standard_core_recruitment_review_thaded/aka_test_oracle_563.yml"
          )
        ),
        input_ports = None
    )
    wf_standard_core_recruitment_review_thaded__alteryxselect_589 = Process(
        name = "wf_standard_core_recruitment_review_thaded__AlteryxSelect_589",
        properties = ModelTransform(modelName = "wf_standard_core_recruitment_review_thaded__AlteryxSelect_589")
    )
    wf_standard_core_recruitment_review_thaded__formula_395_0 = Process(
        name = "wf_standard_core_recruitment_review_thaded__Formula_395_0",
        properties = ModelTransform(modelName = "wf_standard_core_recruitment_review_thaded__Formula_395_0")
    )
    aka_test_oracle_356 = Process(
        name = "aka_test_oracle_356",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:test_oracle",
              "username": "${username_aka_test_oracle_356}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_test_oracle_356"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select *\nfrom\n--gpdip_uddl_dna.standard_core_trends_thaded\nprophecy.standard_core_trends_thaded\nwhere (\"curve_grain\" = 'Study' and \"standard_visit_name\" = 'Randomization' and \"cohort_name\"='All Participants')\nor (\"curve_grain\" = 'Site' and \"standard_visit_name\" = 'Site Activated')"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/wf_standard_core_recruitment_review_thaded/aka_test_oracle_356.yml"
          )
        ),
        input_ports = None
    )
    dynamicinput_437 = Process(
        name = "DynamicInput_437",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [{"textToReplace" : "'C4591001'", "textToReplaceField" : "study_list"}],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "select *\nfrom public.diversity_dashboard_view\nwhere protocols in ('C4591001')",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    aka_test_oracle_448 = Process(
        name = "aka_test_oracle_448",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:test_oracle",
              "username": "${username_aka_test_oracle_448}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_test_oracle_448"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select\n--gpdip_uddl_dna.executive_recruitment_review_rms.study_number_pfe,\n--gpdip_uddl_dna.executive_recruitment_review_rms.recruitment_review_scope_flag\n\"study_number_pfe\",\n\"recruitment_review_scope_flag\"\nfrom\n--gpdip_uddl_dna.executive_recruitment_review_rms\nprophecy.executive_recruitment_review_rms\nwhere \"recruitment_review_scope_flag\" = 'IN SCOPE'"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/wf_standard_core_recruitment_review_thaded/aka_test_oracle_448.yml"
          )
        ),
        input_ports = None
    )
    wf_standard_core_recruitment_review_thaded__formula_374_1 = Process(
        name = "wf_standard_core_recruitment_review_thaded__Formula_374_1",
        properties = ModelTransform(modelName = "wf_standard_core_recruitment_review_thaded__Formula_374_1"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    textinput_548 = Process(
        name = "TextInput_548",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_548", sourceType = "Seed")
        ),
        input_ports = None
    )
    aka_test_oracle_428 = Process(
        name = "aka_test_oracle_428",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:test_oracle",
              "username": "${username_aka_test_oracle_428}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_test_oracle_428"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select\n--gpdip_prd.gpdip_uddl_dna.standard_core_studylist_aggregation_v.*\nprophecy.standard_core_studylist_aggregation_v.*\nfrom\n--gpdip_prd.gpdip_uddl_dna.standard_core_studylist_aggregation_v\nprophecy.standard_core_studylist_aggregation_v"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/wf_standard_core_recruitment_review_thaded/aka_test_oracle_428.yml"
          )
        ),
        input_ports = None
    )
    aka_test_oracle_586 = Process(
        name = "aka_test_oracle_586",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:test_oracle",
              "username": "${username_aka_test_oracle_586}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_test_oracle_586"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select * from\n--gpdip_prd.gpdip_uddl_ar.sv_study_full\nprophecy.sv_study_full"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/wf_standard_core_recruitment_review_thaded/aka_test_oracle_586.yml"
          )
        ),
        input_ports = None
    )
    aka_test_oracle_490 = Process(
        name = "aka_test_oracle_490",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:test_oracle",
              "username": "${username_aka_test_oracle_490}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_test_oracle_490"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select *\nfrom\n--GPDIP_UDDL_DNA.diversity_targets_impala_oc_inform_rms\nprophecy.diversity_targets_impala_oc_inform_rms"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/wf_standard_core_recruitment_review_thaded/aka_test_oracle_490.yml"
          )
        ),
        input_ports = None
    )
    aka_test_oracle_536 = Process(
        name = "aka_test_oracle_536",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:test_oracle",
              "username": "${username_aka_test_oracle_536}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_test_oracle_536"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select *\nfrom\n--public.diversity_dashboard_view\nprophecy.diversity_dashboard_view"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/wf_standard_core_recruitment_review_thaded/aka_test_oracle_536.yml"
          )
        ),
        input_ports = None
    )
    alteryxselect_477 = Process(
        name = "AlteryxSelect_477",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_477", sourceType = "Seed")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_506 = Process(
        name = "aka_GPDIP_EDLUD_506",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:6081259b4d5128137048ca6e",
              "username": "${username_aka_GPDIP_EDLUD_506}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_506"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "select *\nfrom gpdip_uddl_dna.curated_metadata_study_posscs")
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/wf_standard_core_recruitment_review_thaded/aka_GPDIP_EDLUD_506.yml"
          )
        ),
        input_ports = None
    )
    dynamicinput_446 = Process(
        name = "DynamicInput_446",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [{"textToReplace" : "'C4591001'", "textToReplaceField" : "study_list"}],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "select *\nfrom\n--gpdip_uddl_dna.enrollment_v2_dvso_partial_plans_posscs\nprophecy.enrollment_v2_dvso_partial_plans_posscs\n--where STUDY_ID IN ('C4591001')\nwhere \"study_id\" IN ('C4591001')",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    wf_standard_core_recruitment_review_thaded__summarize_432 = Process(
        name = "wf_standard_core_recruitment_review_thaded__Summarize_432",
        properties = ModelTransform(modelName = "wf_standard_core_recruitment_review_thaded__Summarize_432"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    wf_standard_core_recruitment_review_thaded__alteryxselect_590 = Process(
        name = "wf_standard_core_recruitment_review_thaded__AlteryxSelect_590",
        properties = ModelTransform(modelName = "wf_standard_core_recruitment_review_thaded__AlteryxSelect_590")
    )
    wf_standard_core_recruitment_review_thaded__alteryxselect_477_cast = Process(
        name = "wf_standard_core_recruitment_review_thaded__AlteryxSelect_477_cast",
        properties = ModelTransform(modelName = "wf_standard_core_recruitment_review_thaded__AlteryxSelect_477_cast")
    )
    wf_standard_core_recruitment_review_thaded__aka_gpd_uddl_wr_366 = Process(
        name = "wf_standard_core_recruitment_review_thaded__aka_GPD_UDDL_Wr_366",
        properties = ModelTransform(modelName = "wf_standard_core_recruitment_review_thaded__aka_GPD_UDDL_Wr_366"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    dynamicinput_417 = Process(
        name = "DynamicInput_417",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [{"textToReplace" : "'C4591001'", "textToReplaceField" : "study_list"}],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "select *\nfrom\n--gpdip_prd.gpdip_uddl_dna.soqpat_site_file_thaded\nprophecy.soqpat_site_file_thaded\nwhere \"study_id\" in ('C4591001')",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    dynamicinput_446 >> wf_standard_core_recruitment_review_thaded__formula_374_1._in(4)
    aka_gpdip_edlud_506 >> wf_standard_core_recruitment_review_thaded__aka_gpd_uddl_wr_366._in(1)
    (
        wf_standard_core_recruitment_review_thaded__formula_395_0._out(0)
        >> [wf_standard_core_recruitment_review_thaded__aka_gpd_uddl_wr_366._in(5),
              wf_standard_core_recruitment_review_thaded__alteryxselect_589._in(0)]
    )
    (
        wf_standard_core_recruitment_review_thaded__formula_374_1._out(0)
        >> [wf_standard_core_recruitment_review_thaded__alteryxselect_590._in(0),
              wf_standard_core_recruitment_review_thaded__formula_395_0._in(0)]
    )
    (
        aka_test_oracle_428._out(0)
        >> [wf_standard_core_recruitment_review_thaded__summarize_432._in(0),
              wf_standard_core_recruitment_review_thaded__summarize_432._in(1),
              wf_standard_core_recruitment_review_thaded__summarize_432._in(2)]
    )
    aka_test_oracle_586 >> wf_standard_core_recruitment_review_thaded__formula_374_1._in(3)
    alteryxselect_477 >> wf_standard_core_recruitment_review_thaded__alteryxselect_477_cast
    aka_test_oracle_563 >> wf_standard_core_recruitment_review_thaded__aka_gpd_uddl_wr_366._in(2)
    aka_test_oracle_490 >> wf_standard_core_recruitment_review_thaded__aka_gpd_uddl_wr_366._in(0)
    textinput_548 >> wf_standard_core_recruitment_review_thaded__aka_gpd_uddl_wr_366._in(4)
    dynamicinput_417 >> wf_standard_core_recruitment_review_thaded__formula_374_1._in(2)
    (
        wf_standard_core_recruitment_review_thaded__summarize_432._out(0)
        >> [dynamicinput_417._in(0), dynamicinput_437._in(0), dynamicinput_446._in(0)]
    )
    aka_test_oracle_536 >> wf_standard_core_recruitment_review_thaded__aka_gpd_uddl_wr_366._in(6)
    (
        aka_test_oracle_356._out(0)
        >> [wf_standard_core_recruitment_review_thaded__formula_374_1._in(0),
              wf_standard_core_recruitment_review_thaded__formula_374_1._in(1)]
    )
    aka_test_oracle_448 >> wf_standard_core_recruitment_review_thaded__formula_374_1._in(6)
