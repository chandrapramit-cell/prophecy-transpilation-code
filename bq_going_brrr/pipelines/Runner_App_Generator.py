from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Runner_App_Generator",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_AutomaticWorkfl_53 = "''",
      Question__Check_Box_67 = "''",
      YXWZ_Check_Box = "''",
      dynamicSelectExpr55 = "'(CAST(column_name AS STRING) IN ('WorkflowFullPath', 'AYX_RecordID'))'",
      YXMD_Check_Box = "''",
      jdbcUrl_AutomaticWorkfl_53 = "''",
      workflow_name = "'Runner_App_Generator'",
      password_AutomaticWorkfl_53 = "''",
      Question__YXMD_Check_Box = "''",
      Question__YXWZ_Check_Box = "''",
      Question__YXMC_Check_Box = "''",
      YXMC_Check_Box = "''",
      Question__CustomFilterExpression = "''",
      Question__CustomSelection = "''",
      Question__FolderSelection = "''",
      dynamicSelectExpr54 = "'(column_name = 'Field_1')'",
      FolderSelection = "''"
    )
)

with Pipeline(args) as pipeline:
    textinput_8 = Process(
        name = "TextInput_8",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_8", sourceType = "Seed")
        ),
        input_ports = None
    )
    runner_app_generator__sort_78 = Process(
        name = "Runner_App_Generator__Sort_78",
        properties = ModelTransform(modelName = "Runner_App_Generator__Sort_78")
    )
    textinput_4 = Process(
        name = "TextInput_4",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_4", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_9 = Process(
        name = "TextInput_9",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_9", sourceType = "Seed")
        ),
        input_ports = None
    )
    directory_56 = Process(
        name = "Directory_56",
        properties = Directory(
          path = "C:\\DummyDirectory",
          pattern = "*.*",
          integration = "sftp",
          recursive = True,
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {"id" : "transpiled_connection"},
            "type": "connector"
          }
        ),
        input_ports = None
    )
    runner_app_generator__dynamicselect_55 = Process(
        name = "Runner_App_Generator__DynamicSelect_55",
        properties = ModelTransform(modelName = "Runner_App_Generator__DynamicSelect_55")
    )
    runner_app_generator__sort_50 = Process(
        name = "Runner_App_Generator__Sort_50",
        properties = ModelTransform(modelName = "Runner_App_Generator__Sort_50"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17"]
    )
    runner_app_generator__error_79 = Process(
        name = "Runner_App_Generator__Error_79",
        properties = ModelTransform(modelName = "Runner_App_Generator__Error_79"),
        input_ports = None
    )
    textinput_5 = Process(
        name = "TextInput_5",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_5", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_10 = Process(
        name = "TextInput_10",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_10", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_6 = Process(
        name = "TextInput_6",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_6", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_1 = Process(
        name = "TextInput_1",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_1", sourceType = "Seed")
        ),
        input_ports = None
    )
    runner_app_generator__link_69 = Process(
        name = "Runner_App_Generator__Link_69",
        properties = ModelTransform(modelName = "Runner_App_Generator__Link_69"),
        input_ports = None
    )
    runner_app_generator__error_64 = Process(
        name = "Runner_App_Generator__Error_64",
        properties = ModelTransform(modelName = "Runner_App_Generator__Error_64"),
        input_ports = None
    )
    runner_app_generator__automaticworkfl_53 = Process(
        name = "Runner_App_Generator__AutomaticWorkfl_53",
        properties = ModelTransform(modelName = "Runner_App_Generator__AutomaticWorkfl_53"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10"]
    )
    textinput_2 = Process(
        name = "TextInput_2",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_2", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_7 = Process(
        name = "TextInput_7",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_7", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_3 = Process(
        name = "TextInput_3",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_3", sourceType = "Seed")
        ),
        input_ports = None
    )
    runner_app_generator__folderbrowse_74 = Process(
        name = "Runner_App_Generator__FolderBrowse_74",
        properties = ModelTransform(modelName = "Runner_App_Generator__FolderBrowse_74"),
        input_ports = None
    )
    (
        runner_app_generator__dynamicselect_55._out(0)
        >> [runner_app_generator__automaticworkfl_53._in(1), runner_app_generator__sort_50._in(1),
              runner_app_generator__sort_50._in(3), runner_app_generator__sort_50._in(5),
              runner_app_generator__sort_50._in(7), runner_app_generator__sort_50._in(9),
              runner_app_generator__sort_50._in(11)]
    )
    textinput_7 >> runner_app_generator__sort_50._in(17)
    textinput_8 >> runner_app_generator__automaticworkfl_53._in(8)
    directory_56 >> runner_app_generator__dynamicselect_55
    textinput_10 >> runner_app_generator__automaticworkfl_53._in(10)
    textinput_4 >> runner_app_generator__sort_50._in(14)
    textinput_6 >> runner_app_generator__sort_50._in(16)
    textinput_2 >> runner_app_generator__sort_50._in(12)
    textinput_1 >> runner_app_generator__automaticworkfl_53._in(7)
    (
        runner_app_generator__sort_50._out(0)
        >> [runner_app_generator__automaticworkfl_53._in(5), runner_app_generator__sort_78._in(0)]
    )
    textinput_3 >> runner_app_generator__sort_50._in(13)
    textinput_5 >> runner_app_generator__sort_50._in(15)
    textinput_9 >> runner_app_generator__automaticworkfl_53._in(9)
