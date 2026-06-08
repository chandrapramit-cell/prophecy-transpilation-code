from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "alteryx_all_test_stage2_sql",
    version = 1,
    auto_layout = False,
    params = Parameters(WORKFLOW_NAME = "'alteryx_all_test_stage2_sql'")
)

with Pipeline(args) as pipeline:
    textinput_108 = Process(
        name = "TextInput_108",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_alteryx_all_test_stage2_sql_108", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_123 = Process(
        name = "TextInput_123",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_alteryx_all_test_stage2_sql_123", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_126 = Process(
        name = "TextInput_126",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_alteryx_all_test_stage2_sql_126", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_13 = Process(
        name = "TextInput_13",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_alteryx_all_test_stage2_sql_13", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_133 = Process(
        name = "TextInput_133",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_alteryx_all_test_stage2_sql_133", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_141 = Process(
        name = "TextInput_141",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_alteryx_all_test_stage2_sql_141", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_142 = Process(
        name = "TextInput_142",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_alteryx_all_test_stage2_sql_142", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_260 = Process(
        name = "TextInput_260",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_alteryx_all_test_stage2_sql_260", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_75 = Process(
        name = "TextInput_75",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_alteryx_all_test_stage2_sql_75", sourceType = "Seed")
        ),
        input_ports = None
    )
    alteryx_all_tes_259 = Process(
        name = "alteryx_all_tes_259",
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
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "C:\\Users\\prophecy\\Documents\\qa_test_artefacts\\test2\\alteryx_all_test_2ndstage_output.csv"
          )
        )
    )
    alteryx_all_test_stage2_sql__regex_245_rename_0 = Process(
        name = "alteryx_all_test_stage2_sql__RegEx_245_rename_0",
        properties = ModelTransform(modelName = "alteryx_all_test_stage2_sql__RegEx_245_rename_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8"]
    )
    textinput_126 >> alteryx_all_test_stage2_sql__regex_245_rename_0._in(4)
    textinput_141 >> alteryx_all_test_stage2_sql__regex_245_rename_0._in(6)
    textinput_75 >> alteryx_all_test_stage2_sql__regex_245_rename_0._in(1)
    alteryx_all_test_stage2_sql__regex_245_rename_0 >> alteryx_all_tes_259
    textinput_108 >> alteryx_all_test_stage2_sql__regex_245_rename_0._in(2)
    textinput_123 >> alteryx_all_test_stage2_sql__regex_245_rename_0._in(3)
    textinput_133 >> alteryx_all_test_stage2_sql__regex_245_rename_0._in(5)
    textinput_260 >> alteryx_all_test_stage2_sql__regex_245_rename_0._in(8)
    textinput_13 >> alteryx_all_test_stage2_sql__regex_245_rename_0._in(0)
    textinput_142 >> alteryx_all_test_stage2_sql__regex_245_rename_0._in(7)
