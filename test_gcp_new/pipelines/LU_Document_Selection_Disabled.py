from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "LU_Document_Selection_Disabled",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_GPDIP_EDLUD_298 = "''",
      password_aka_GPDIP_EDLUD_298 = "''",
      username_aka_GPDIP_EDLUD_289 = "''",
      password_aka_GPDIP_EDLUD_289 = "''",
      username_aka_GPDIP_EDLUD_302 = "''",
      password_aka_GPDIP_EDLUD_302 = "''",
      username_aka_GPDIP_EDLUD_312 = "''",
      password_aka_GPDIP_EDLUD_312 = "''",
      username_aka_GPDIP_EDLUD_324 = "''",
      password_aka_GPDIP_EDLUD_324 = "''",
      jdbcUrl_aka_GPD_UDDL_Wr_295 = "''",
      username_aka_GPD_UDDL_Wr_295 = "''",
      password_aka_GPD_UDDL_Wr_295 = "''",
      username_aka_GPDIP_EDLUD_319 = "''",
      password_aka_GPDIP_EDLUD_319 = "''",
      username_aka_GPDIP_EDLUD_307 = "''",
      password_aka_GPDIP_EDLUD_307 = "''",
      workflow_name = "'LU_Document_Selection_Disabled'"
    )
)

with Pipeline(args) as pipeline:
    lu_document_selection_disabled__join_308_inner = Process(
        name = "LU_Document_Selection_Disabled__Join_308_inner",
        properties = ModelTransform(modelName = "LU_Document_Selection_Disabled__Join_308_inner"),
        input_ports = ["in_0", "in_1"]
    )
    lu_document_selection_disabled__join_320_inner = Process(
        name = "LU_Document_Selection_Disabled__Join_320_inner",
        properties = ModelTransform(modelName = "LU_Document_Selection_Disabled__Join_320_inner"),
        input_ports = ["in_0", "in_1"]
    )
    lu_document_selection_disabled__aka_gpd_uddl_wr_295 = Process(
        name = "LU_Document_Selection_Disabled__aka_GPD_UDDL_Wr_295",
        properties = ModelTransform(modelName = "LU_Document_Selection_Disabled__aka_GPD_UDDL_Wr_295"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    aka_gpdip_edlud_289 = Process(
        name = "aka_GPDIP_EDLUD_289",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_289}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_289"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT app_country_id\n       , country_name\n       , country_abbreviation\n       , product_name\n       , is_static\n       , is_active\n       , extract_date\n FROM gpdip_prd.gpdip_uddl_regulatory.pfleet_country\nWHERE parent_app_country_id is not null and is_active = 1"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Document_Selection_Disabled/aka_GPDIP_EDLUD_289.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_298 = Process(
        name = "aka_GPDIP_EDLUD_298",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_298}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_298"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT r_object_id\n       , i_chronicle_id\n       , object_name\n       , title\n       , xm_status\n       , i_has_folder\n       , subtype\n       , xm_language\n FROM gpdip_prd.gpdip_uddl_regulatory.gdms_coredatasheet_single\nWHERE i_has_folder = 1"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Document_Selection_Disabled/aka_GPDIP_EDLUD_298.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_302 = Process(
        name = "aka_GPDIP_EDLUD_302",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_302}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_302"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT r_object_id\n       , i_position\n       , name\n       , value\n FROM gpdip_prd.gpdip_uddl_regulatory.gdms_coredatasheet_repeating \n where name in ('PFLEET_SUB_COUNTRY_ROW_ID','R_VERSION_LABEL');"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Document_Selection_Disabled/aka_GPDIP_EDLUD_302.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_307 = Process(
        name = "aka_GPDIP_EDLUD_307",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_307}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_307"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT app_country_id as sub_country_id\n       , i_chronicle_id\n FROM gpdip_prd.gpdip_uddl_regulatory.lu_temp_core_chronicle_ids\nWHERE is_active = 1"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Document_Selection_Disabled/aka_GPDIP_EDLUD_307.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_312 = Process(
        name = "aka_GPDIP_EDLUD_312",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_312}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_312"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT r_object_id\n       , i_chronicle_id\n       , object_name\n       , title\n       , xm_status\n       , i_has_folder\n       , subtype\n       , xm_language\n FROM gpdip_prd.gpdip_uddl_regulatory.gdms_prescribinginfo_single\nWHERE i_has_folder = 1"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Document_Selection_Disabled/aka_GPDIP_EDLUD_312.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_319 = Process(
        name = "aka_GPDIP_EDLUD_319",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_319}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_319"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT app_country_id as sub_country_id\n       , i_chronicle_id\n FROM gpdip_prd.gpdip_uddl_regulatory.lu_temp_country_chronicle_ids\nWHERE is_active = 1"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Document_Selection_Disabled/aka_GPDIP_EDLUD_319.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_324 = Process(
        name = "aka_GPDIP_EDLUD_324",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_324}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_324"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT r_object_id\n       , i_position\n       , name\n       , value\n FROM gpdip_prd.gpdip_uddl_regulatory.gdms_prescribinginfo_repeating\n where name in ('PFLEET_SUB_COUNTRY_ROW_ID','R_VERSION_LABEL');"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Document_Selection_Disabled/aka_GPDIP_EDLUD_324.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_307 >> lu_document_selection_disabled__join_308_inner._in(1)
    (
        aka_gpdip_edlud_298._out(0)
        >> [lu_document_selection_disabled__aka_gpd_uddl_wr_295._in(5),
              lu_document_selection_disabled__join_308_inner._in(0)]
    )
    (
        aka_gpdip_edlud_324._out(0)
        >> [lu_document_selection_disabled__aka_gpd_uddl_wr_295._in(3),
              lu_document_selection_disabled__aka_gpd_uddl_wr_295._in(4)]
    )
    (
        aka_gpdip_edlud_302._out(0)
        >> [lu_document_selection_disabled__aka_gpd_uddl_wr_295._in(1),
              lu_document_selection_disabled__aka_gpd_uddl_wr_295._in(2)]
    )
    (
        aka_gpdip_edlud_312._out(0)
        >> [lu_document_selection_disabled__aka_gpd_uddl_wr_295._in(6),
              lu_document_selection_disabled__join_320_inner._in(0)]
    )
    aka_gpdip_edlud_289 >> lu_document_selection_disabled__aka_gpd_uddl_wr_295._in(0)
    aka_gpdip_edlud_319 >> lu_document_selection_disabled__join_320_inner._in(1)
