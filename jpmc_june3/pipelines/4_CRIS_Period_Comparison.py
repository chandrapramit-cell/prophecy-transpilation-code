from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "4_CRIS_Period_Comparison",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_GPDIP_EDLUD_150 = "''",
      password_aka_GPDIP_EDLUD_150 = "''",
      username_aka_GPDIP_EDLUD_151 = "''",
      password_aka_GPDIP_EDLUD_151 = "''",
      username_aka_GPDIP_EDLUD_172 = "''",
      password_aka_GPDIP_EDLUD_172 = "''",
      username_aka_GPDIP_EDLUD_171 = "''",
      password_aka_GPDIP_EDLUD_171 = "''",
      jdbcUrl_aka_GPD_UDDL_Wr_160 = "''",
      username_aka_GPD_UDDL_Wr_160 = "''",
      password_aka_GPD_UDDL_Wr_160 = "''",
      workflow_name = "'4_CRIS_Period_Comparison'"
    )
)

with Pipeline(args) as pipeline:
    node_4_cris_period_comparison__aka_gpd_uddl_wr_160 = Process(
        name = "4_CRIS_Period_Comparison__aka_GPD_UDDL_Wr_160",
        properties = ModelTransform(modelName = "4_CRIS_Period_Comparison__aka_GPD_UDDL_Wr_160"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14"]
    )
    aka_gpdip_edlud_150 = Process(
        name = "aka_GPDIP_EDLUD_150",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d9caf674d51281464f456e2",
              "username": "${username_aka_GPDIP_EDLUD_150}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_150"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "Select * From gpdip_prd.gpdip_uddl_hcdb.hc_all_users")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/4_CRIS_Period_Comparison/aka_GPDIP_EDLUD_150.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_151 = Process(
        name = "aka_GPDIP_EDLUD_151",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d9caf674d51281464f456e2",
              "username": "${username_aka_GPDIP_EDLUD_151}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_151"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "Select A.* From gpdip_uddl_hcdb.hc_all_users_archive A Where trunc(A.period) = (Select Max(t_period) From (Select Distinct trunc(gpdip_uddl_hcdb.hc_all_users_archive.period) As t_period From gpdip_uddl_hcdb.hc_all_users_archive))"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/4_CRIS_Period_Comparison/aka_GPDIP_EDLUD_151.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_171 = Process(
        name = "aka_GPDIP_EDLUD_171",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d9caf674d51281464f456e2",
              "username": "${username_aka_GPDIP_EDLUD_171}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_171"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "SELECT * FROM \"gpdip_prd\".\"gpdip_uddl_hcdb\".\"hc_users_rwt\"")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/4_CRIS_Period_Comparison/aka_GPDIP_EDLUD_171.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_172 = Process(
        name = "aka_GPDIP_EDLUD_172",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5d9caf674d51281464f456e2",
              "username": "${username_aka_GPDIP_EDLUD_172}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_GPDIP_EDLUD_172"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "Select A.* From gpdip_uddl_hcdb.hc_users_rwt_archive A Where trunc(A.period) = (Select Max(t_period) From (Select Distinct trunc(gpdip_uddl_hcdb.hc_users_rwt_archive.period) As t_period From gpdip_uddl_hcdb.hc_users_rwt_archive))"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/4_CRIS_Period_Comparison/aka_GPDIP_EDLUD_172.yml")
        ),
        input_ports = None
    )
    aka_gpdip_edlud_171 >> node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(0)
    aka_gpdip_edlud_172 >> node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(1)
    (
        aka_gpdip_edlud_150._out(0)
        >> [node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(2),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(3),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(6),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(7),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(8),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(9),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(12),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(13)]
    )
    (
        aka_gpdip_edlud_151._out(0)
        >> [node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(4),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(5),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(10),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(11),
              node_4_cris_period_comparison__aka_gpd_uddl_wr_160._in(14)]
    )
