from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "LU_Product_Selection_Disabled",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_GPDIP_EDLUD_11 = "''",
      password_aka_GPDIP_EDLUD_11 = "''",
      username_aka_GPDIP_EDLUD_113 = "''",
      password_aka_GPDIP_EDLUD_113 = "''",
      username_aka_GPDIP_EDLUD_9 = "''",
      password_aka_GPDIP_EDLUD_9 = "''",
      username_aka_GPDIP_EDLUD_133 = "''",
      password_aka_GPDIP_EDLUD_133 = "''",
      username_aka_GPDIP_EDLUD_132 = "''",
      password_aka_GPDIP_EDLUD_132 = "''",
      jdbcUrl_lu_product_sele_95 = "''",
      username_lu_product_sele_95 = "''",
      password_lu_product_sele_95 = "''",
      jdbcUrl_aka_GPD_UDDL_Wr_92 = "''",
      username_aka_GPD_UDDL_Wr_92 = "''",
      password_aka_GPD_UDDL_Wr_92 = "''",
      workflow_name = "'LU_Product_Selection_Disabled'"
    )
)

with Pipeline(args) as pipeline:
    lu_product_selection_disabled__join_173_inner = Process(
        name = "LU_Product_Selection_Disabled__Join_173_inner",
        properties = ModelTransform(modelName = "LU_Product_Selection_Disabled__Join_173_inner"),
        input_ports = None
    )
    lu_product_selection_disabled__aka_gpd_uddl_wr_92 = Process(
        name = "LU_Product_Selection_Disabled__aka_GPD_UDDL_Wr_92",
        properties = ModelTransform(modelName = "LU_Product_Selection_Disabled__aka_GPD_UDDL_Wr_92"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    aka_gpdip_edlud_11 = Process(
        name = "aka_GPDIP_EDLUD_11",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_11}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_11"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "Select * From gpdip_prd.gpdip_uddl_regulatory.gdms_coredatasheet_repeating"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Product_Selection_Disabled/aka_GPDIP_EDLUD_11.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_113 = Process(
        name = "aka_GPDIP_EDLUD_113",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_113}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_113"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT app_country_id\n       , country_name\n       , country_abbreviation\n       , product_name\n       , parent_app_country_id\n       , is_static\n       , is_active\n FROM gpdip_prd.gpdip_uddl_regulatory.pfleet_country\nWHERE  is_active = 1"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Product_Selection_Disabled/aka_GPDIP_EDLUD_113.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_132 = Process(
        name = "aka_GPDIP_EDLUD_132",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_132}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_132"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT * FROM gpdip_prd.gpdip_uddl_regulatory.gdms_prescribinginfo_repeating"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Product_Selection_Disabled/aka_GPDIP_EDLUD_132.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_133 = Process(
        name = "aka_GPDIP_EDLUD_133",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_133}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_133"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT * FROM gpdip_prd.gpdip_uddl_regulatory.gdms_prescribinginfo_single"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/LU_Product_Selection_Disabled/aka_GPDIP_EDLUD_133.yml"
          )
        ),
        input_ports = None
    )
    aka_gpdip_edlud_9 = Process(
        name = "aka_GPDIP_EDLUD_9",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d5fcee94d51281464f2fed5",
              "username": "${username_aka_GPDIP_EDLUD_9}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_9"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "Select * From gpdip_prd.gpdip_uddl_regulatory.gdms_coredatasheet_single"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/LU_Product_Selection_Disabled/aka_GPDIP_EDLUD_9.yml")
        ),
        input_ports = None
    )
    lu_product_sele_95 = Process(
        name = "lu_product_sele_95",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(filePath = "_externals\\1\\lu_product_selection_output.xlsx")
        )
    )
    (
        aka_gpdip_edlud_11._out(0)
        >> [lu_product_selection_disabled__aka_gpd_uddl_wr_92._in(4),
              lu_product_selection_disabled__aka_gpd_uddl_wr_92._in(5)]
    )
    (
        aka_gpdip_edlud_132._out(0)
        >> [lu_product_selection_disabled__aka_gpd_uddl_wr_92._in(0),
              lu_product_selection_disabled__aka_gpd_uddl_wr_92._in(1)]
    )
    aka_gpdip_edlud_113 >> lu_product_selection_disabled__aka_gpd_uddl_wr_92._in(6)
    lu_product_selection_disabled__aka_gpd_uddl_wr_92 >> lu_product_sele_95
    aka_gpdip_edlud_133 >> lu_product_selection_disabled__aka_gpd_uddl_wr_92._in(2)
    aka_gpdip_edlud_9 >> lu_product_selection_disabled__aka_gpd_uddl_wr_92._in(3)
