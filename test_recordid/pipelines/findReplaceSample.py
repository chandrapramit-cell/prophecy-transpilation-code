from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "findReplaceSample",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'findReplaceSample'")
)

with Pipeline(args) as pipeline:
    textinput_1 = Process(
        name = "TextInput_1",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_1", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_2 = Process(
        name = "TextInput_2",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_2", sourceType = "Seed")
        ),
        input_ports = None
    )
    findreplacesample__findreplace_3_reorg_0 = Process(
        name = "findReplaceSample__FindReplace_3_reorg_0",
        properties = ModelTransform(modelName = "findReplaceSample__FindReplace_3_reorg_0"),
        input_ports = ["in_0", "in_1"]
    )
    output_csv_4 = Process(
        name = "output_csv_4",
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
          format = SFTPTarget.CsvWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(filePath = "output.csv")
        )
    )
    textinput_1 >> findreplacesample__findreplace_3_reorg_0._in(0)
    textinput_2 >> findreplacesample__findreplace_3_reorg_0._in(1)
    findreplacesample__findreplace_3_reorg_0 >> output_csv_4
