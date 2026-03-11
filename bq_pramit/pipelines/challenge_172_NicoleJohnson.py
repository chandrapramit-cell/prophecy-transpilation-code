from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "challenge_172_NicoleJohnson",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_TopBlocks_yxdb_23 = "''",
      password_TopBlocks_yxdb_23 = "''",
      workflow_name = "'challenge_172_NicoleJohnson'"
    )
)

with Pipeline(args) as pipeline:
    topblocks_yxdb_23 = Process(
        name = "TopBlocks_yxdb_23",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "Top Blocks.yxdb",
              "username": "${username_TopBlocks_yxdb_23}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "TopBlocks_yxdb_23"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "Top Blocks.yxdb")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/challenge_172_NicoleJohnson/TopBlocks_yxdb_23.yml")
        ),
        input_ports = None
    )
    textinput_48 = Process(
        name = "TextInput_48",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_48", sourceType = "Seed")
        ),
        input_ports = None
    )
    challenge_172_nicolejohnson__textinput_48_cast = Process(
        name = "challenge_172_NicoleJohnson__TextInput_48_cast",
        properties = ModelTransform(modelName = "challenge_172_NicoleJohnson__TextInput_48_cast")
    )
    challenge_172_nicolejohnson__macro_52 = Process(
        name = "challenge_172_NicoleJohnson__Macro_52",
        properties = ModelTransform(modelName = "challenge_172_NicoleJohnson__Macro_52"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    (
        topblocks_yxdb_23._out(0)
        >> [challenge_172_nicolejohnson__macro_52._in(0), challenge_172_nicolejohnson__macro_52._in(2)]
    )
    textinput_48 >> challenge_172_nicolejohnson__textinput_48_cast
