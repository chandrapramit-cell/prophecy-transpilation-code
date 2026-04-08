from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "subject_workflow",
    version = 1,
    auto_layout = False,
    params = Parameters(
      USERNAME_AKA_GPDIP_EDLUD_443 = "''",
      PASSWORD_AKA_GPDIP_EDLUD_443 = "''",
      USERNAME_AKA_GPDIP_EDLUD_369 = "''",
      PASSWORD_AKA_GPDIP_EDLUD_369 = "''",
      USERNAME_AKA_GPDIP_EDLUD_437 = "''",
      PASSWORD_AKA_GPDIP_EDLUD_437 = "''",
      USERNAME_AKA_GPDIP_EDLUD_368 = "''",
      PASSWORD_AKA_GPDIP_EDLUD_368 = "''",
      USERNAME_AKA_GPDIP_EDLUD_418 = "''",
      PASSWORD_AKA_GPDIP_EDLUD_418 = "''",
      USERNAME_AKA_ENIP164_QUE_419 = "''",
      PASSWORD_AKA_ENIP164_QUE_419 = "''",
      USERNAME_AKA_GPDIP_EDLUD_376 = "''",
      PASSWORD_AKA_GPDIP_EDLUD_376 = "''",
      JDBCURL_AKA_GPD_UDDL_WR_373 = "''",
      USERNAME_AKA_GPD_UDDL_WR_373 = "''",
      PASSWORD_AKA_GPD_UDDL_WR_373 = "''",
      USERNAME_AKA_GPDIP_EDLUD_428 = "''",
      PASSWORD_AKA_GPDIP_EDLUD_428 = "''",
      WORKFLOW_NAME = "'subject_workflow'"
    )
)

with Pipeline(args) as pipeline:
    subject_workflow__filter_381 = Process(
        name = "subject_workflow__Filter_381",
        properties = ModelTransform(modelName = "subject_workflow__Filter_381")
    )
    subject_workflow__filter_381_reject = Process(
        name = "subject_workflow__Filter_381_reject",
        properties = ModelTransform(modelName = "subject_workflow__Filter_381_reject")
    )
    subject_workflow__formula_208_0 = Process(
        name = "subject_workflow__Formula_208_0",
        properties = ModelTransform(modelName = "subject_workflow__Formula_208_0")
    )
    aka_gpdip_edlud_418 = Process(
        name = "aka_GPDIP_EDLUD_418",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:6081259b4d5128137048ca6e",
              "username": "${username_aka_GPDIP_EDLUD_418}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_418"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "Select distinct study_id, study_alias\nfrom gpdip_cdm.study_alias_v")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/subject_workflow/aka_GPDIP_EDLUD_418.yml")
        ),
        input_ports = None
    )
    aka_gpd_uddl_wr_373 = Process(
        name = "aka_GPD_UDDL_Wr_373",
        properties = DatabricksTarget(
          connector = {
            "kind": "Databricks",
            "id": "transpiled_connection",
            "properties": {
              "catalog": "transpiled_catalog",
              "clientId": "transpiled_client_id",
              "authType": "token",
              "id": "transpiled_connection",
              "schema": "transpiled_schema",
              "jdbcUrl": "transpiled_jdbc_url",
              "token": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_token_secret", "value" : "transpiled_token_secret"},
                "subKind": "text",
                "type": "secret"
              },
              "clientSecret": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_client_secret", "value" : "transpiled_client_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = DatabricksTarget.DatabricksTargetInternal(
            tableFullName = DatabricksTarget.WarehouseTableName(name = "aka_GPD_UDDL_Wr_373")
          ),
          format = DatabricksTarget.DatabricksWriteFormat()
        )
    )
    subject_workflow__joinmultiple_15 = Process(
        name = "subject_workflow__JoinMultiple_15",
        properties = ModelTransform(modelName = "subject_workflow__JoinMultiple_15")
    )
    subject_workflow__dynamicrename_351 = Process(
        name = "subject_workflow__DynamicRename_351",
        properties = ModelTransform(modelName = "subject_workflow__DynamicRename_351")
    )
    textinput_212 = Process(
        name = "TextInput_212",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_212", sourceType = "Seed")
        ),
        input_ports = None
    )
    subject_workflow__alteryxselect_26 = Process(
        name = "subject_workflow__AlteryxSelect_26",
        properties = ModelTransform(modelName = "subject_workflow__AlteryxSelect_26")
    )
    subject_workflow__alteryxselect_429 = Process(
        name = "subject_workflow__AlteryxSelect_429",
        properties = ModelTransform(modelName = "subject_workflow__AlteryxSelect_429")
    )
    aka_gpdip_edlud_437 = Process(
        name = "aka_GPDIP_EDLUD_437",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:6081259b4d5128137048ca6e",
              "username": "${username_aka_GPDIP_EDLUD_437}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_437"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select * \nfrom gpdip_cdm.subject_v WHERE (SUBJ_STATUS NOT IN ('DELETED')\nOR SUBJ_STATUS IS NULL)\nAND DELETE_FLAG = 'N'"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/subject_workflow/aka_GPDIP_EDLUD_437.yml")
        ),
        input_ports = None
    )
    subject_workflow__formula_433_to_formula_380_0 = Process(
        name = "subject_workflow__Formula_433_to_Formula_380_0",
        properties = ModelTransform(modelName = "subject_workflow__Formula_433_to_Formula_380_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14"]
    )
    subject_workflow__join_288_left_unionfullouter = Process(
        name = "subject_workflow__Join_288_left_UnionFullOuter",
        properties = ModelTransform(modelName = "subject_workflow__Join_288_left_UnionFullOuter"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    aka_enip164_que_419 = Process(
        name = "aka_enip164_Que_419",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5f044c484d51284afc63a84a",
              "username": "${username_aka_enip164_Que_419}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_enip164_Que_419"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "SO_OWNER.STG_SUBJECT_EVENT_OVERRIDE")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/subject_workflow/aka_enip164_Que_419.yml")
        ),
        input_ports = None
    )
    subject_workflow__alteryxselect_213 = Process(
        name = "subject_workflow__AlteryxSelect_213",
        properties = ModelTransform(modelName = "subject_workflow__AlteryxSelect_213")
    )
    aka_gpdip_edlud_428 = Process(
        name = "aka_GPDIP_EDLUD_428",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:6081259b4d5128137048ca6e",
              "username": "${username_aka_GPDIP_EDLUD_428}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_428"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select distinct study_id,cohort_name,cohort_id,subj_strata_num,subj_cohort_num,label_type\nfrom gpdip_uddl_dna.enrollment_planning_curves_thaded\nwhere cohort_name != 'All Participants'"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/subject_workflow/aka_GPDIP_EDLUD_428.yml")
        ),
        input_ports = None
    )
    subject_workflow__sort_348 = Process(
        name = "subject_workflow__Sort_348",
        properties = ModelTransform(modelName = "subject_workflow__Sort_348"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    subject_workflow__alteryxselect_25 = Process(
        name = "subject_workflow__AlteryxSelect_25",
        properties = ModelTransform(modelName = "subject_workflow__AlteryxSelect_25")
    )
    aka_gpdip_edlud_368 = Process(
        name = "aka_GPDIP_EDLUD_368",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:6081259b4d5128137048ca6e",
              "username": "${username_aka_GPDIP_EDLUD_368}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_368"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select distinct study_id, study_site_number\nfrom gpdip_prd.gpdip_cdm.study_site"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/subject_workflow/aka_GPDIP_EDLUD_368.yml")
        ),
        input_ports = None
    )
    subject_workflow__join_210_inner = Process(
        name = "subject_workflow__Join_210_inner",
        properties = ModelTransform(modelName = "subject_workflow__Join_210_inner"),
        input_ports = ["in_0", "in_1"]
    )
    aka_gpdip_edlud_376 = Process(
        name = "aka_GPDIP_EDLUD_376",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:6081259b4d5128137048ca6e",
              "username": "${username_aka_GPDIP_EDLUD_376}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_376"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select study_id, study_site_number, country_name\nfrom gpdip_prd.gpdip_uddl_dna.soqpat_site_file_thaded"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/subject_workflow/aka_GPDIP_EDLUD_376.yml")
        ),
        input_ports = None
    )
    subject_workflow__join_194_left = Process(
        name = "subject_workflow__Join_194_left",
        properties = ModelTransform(modelName = "subject_workflow__Join_194_left"),
        input_ports = ["in_0", "in_1"]
    )
    subject_workflow__alteryxselect_20 = Process(
        name = "subject_workflow__AlteryxSelect_20",
        properties = ModelTransform(modelName = "subject_workflow__AlteryxSelect_20")
    )
    aka_gpdip_edlud_443 = Process(
        name = "aka_GPDIP_EDLUD_443",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:6081259b4d5128137048ca6e",
              "username": "${username_aka_GPDIP_EDLUD_443}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_443"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "select *\nfrom GPDIP_UDDL_DNA.SES_COHORT_MAPPING_THADED")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/subject_workflow/aka_GPDIP_EDLUD_443.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_369 = Process(
        name = "aka_GPDIP_EDLUD_369",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:6081259b4d5128137048ca6e",
              "username": "${username_aka_GPDIP_EDLUD_369}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_369"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "select *\nfrom gpdip_uddl_dna.enrollment_v2_subject_visit_posscs")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/subject_workflow/aka_GPDIP_EDLUD_369.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_368 >> subject_workflow__join_288_left_unionfullouter._in(1)
    aka_gpdip_edlud_443 >> subject_workflow__formula_433_to_formula_380_0._in(3)
    (
        subject_workflow__join_288_left_unionfullouter._out(0)
        >> [subject_workflow__formula_433_to_formula_380_0._in(11),
              subject_workflow__formula_433_to_formula_380_0._in(12),
              subject_workflow__join_194_left._in(1)]
    )
    (
        subject_workflow__joinmultiple_15._out(0)
        >> [subject_workflow__dynamicrename_351._in(0), subject_workflow__filter_381._in(0),
              subject_workflow__filter_381_reject._in(0)]
    )
    aka_gpdip_edlud_376 >> subject_workflow__formula_433_to_formula_380_0._in(13)
    aka_gpdip_edlud_428 >> subject_workflow__alteryxselect_429
    aka_enip164_que_419 >> subject_workflow__formula_433_to_formula_380_0._in(2)
    (
        subject_workflow__alteryxselect_213._out(0)
        >> [subject_workflow__join_288_left_unionfullouter._in(2), subject_workflow__join_210_inner._in(0)]
    )
    (
        aka_gpdip_edlud_369._out(0)
        >> [subject_workflow__formula_433_to_formula_380_0._in(0),
              subject_workflow__formula_433_to_formula_380_0._in(1),
              subject_workflow__joinmultiple_15._in(0), subject_workflow__alteryxselect_20._in(0)]
    )
    aka_gpdip_edlud_418 >> subject_workflow__formula_433_to_formula_380_0._in(9)
    (
        subject_workflow__join_194_left._out(0)
        >> [subject_workflow__sort_348._in(0), subject_workflow__sort_348._in(1),
              subject_workflow__formula_433_to_formula_380_0._in(4)]
    )
    (
        subject_workflow__dynamicrename_351._out(0)
        >> [subject_workflow__formula_433_to_formula_380_0._in(5),
              subject_workflow__formula_433_to_formula_380_0._in(7),
              subject_workflow__join_194_left._in(0)]
    )
    subject_workflow__formula_433_to_formula_380_0 >> aka_gpd_uddl_wr_373
    textinput_212 >> subject_workflow__alteryxselect_213
    (
        subject_workflow__formula_208_0._out(0)
        >> [subject_workflow__join_288_left_unionfullouter._in(3), subject_workflow__join_210_inner._in(1)]
    )
    (
        subject_workflow__alteryxselect_20._out(0)
        >> [subject_workflow__alteryxselect_25._in(0), subject_workflow__alteryxselect_26._in(0)]
    )
    aka_gpdip_edlud_437 >> subject_workflow__formula_208_0
