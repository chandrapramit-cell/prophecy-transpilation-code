from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Challenge_356_solution",
    version = 1,
    auto_layout = False,
    params = Parameters(USERNAME_WORDS_TXT_130 = "''", PASSWORD_WORDS_TXT_130 = "''", WORKFLOW_NAME = "'Challenge_356_solution'")
)

with Pipeline(args) as pipeline:
    challenge_356_solution__sort_153 = Process(
        name = "Challenge_356_solution__Sort_153",
        properties = ModelTransform(modelName = "Challenge_356_solution__Sort_153"),
        input_ports = ["in_0", "in_1"]
    )
    challenge_356_solution__textinput_159_cast = Process(
        name = "Challenge_356_solution__TextInput_159_cast",
        properties = ModelTransform(modelName = "Challenge_356_solution__TextInput_159_cast")
    )
    textinput_131 = Process(
        name = "TextInput_131",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_131", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_159 = Process(
        name = "TextInput_159",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_159", sourceType = "Seed")
        ),
        input_ports = None
    )
    words_txt_130 = Process(
        name = "words_txt_130",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "words.txt",
              "username": "${username_words_txt_130}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "words_txt_130"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "words.txt")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/Challenge_356_solution/words_txt_130.yml")
        ),
        input_ports = None
    )
    textinput_131 >> challenge_356_solution__sort_153._in(0)
    words_txt_130 >> challenge_356_solution__sort_153._in(1)
    textinput_159 >> challenge_356_solution__textinput_159_cast
