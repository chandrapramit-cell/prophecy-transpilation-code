from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "boeing",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_Database__REPOR_1 = "''",
      password_Database__REPOR_1 = "''",
      username_Database__LOADI_111 = "''",
      password_Database__REPOR_84 = "''",
      workflow_name = "'boeing'",
      username_Database__LOADI_98 = "''",
      username_Database__Repor_8 = "''",
      jdbcUrl_Database__LOADI_111 = "''",
      password_Database__Repor_8 = "''",
      username_Database__REPOR_84 = "''",
      username_Database__REPOR_36 = "''",
      password_Database__LOADI_98 = "''",
      jdbcUrl_Database__LOADI_98 = "''",
      username_Database__REPOR_2 = "''",
      password_Database__REPOR_36 = "''",
      password_Database__LOADI_111 = "''",
      password_Database__REPOR_2 = "''"
    )
)

with Pipeline(args) as pipeline:
    boeing__database__loadi_98 = Process(
        name = "boeing__Database__LOADI_98",
        properties = ModelTransform(modelName = "boeing__Database__LOADI_98")
    )
    boeing__filter_68 = Process(name = "boeing__Filter_68", properties = ModelTransform(modelName = "boeing__Filter_68"))
    boeing__alteryxselect_25 = Process(
        name = "boeing__AlteryxSelect_25",
        properties = ModelTransform(modelName = "boeing__AlteryxSelect_25"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    boeing__database__loadi_111 = Process(
        name = "boeing__Database__LOADI_111",
        properties = ModelTransform(modelName = "boeing__Database__LOADI_111"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    boeing__unique_97 = Process(name = "boeing__Unique_97", properties = ModelTransform(modelName = "boeing__Unique_97"))
    boeing__formula_7_0 = Process(
        name = "boeing__Formula_7_0",
        properties = ModelTransform(modelName = "boeing__Formula_7_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    boeing__formula_78_0 = Process(
        name = "boeing__Formula_78_0",
        properties = ModelTransform(modelName = "boeing__Formula_78_0"),
        input_ports = ["in_0", "in_1"]
    )
    textinput_41 = Process(
        name = "TextInput_41",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_41", sourceType = "Seed")
        ),
        input_ports = None
    )
    boeing__alteryxselect_25._out(0) >> [boeing__unique_97._in(0), boeing__formula_78_0._in(1)]
    boeing__formula_7_0._out(0) >> [boeing__database__loadi_111._in(3), boeing__filter_68._in(0)]
    boeing__unique_97._out(0) >> [boeing__database__loadi_111._in(0), boeing__database__loadi_98._in(0)]
    textinput_41 >> boeing__formula_7_0._in(1)
    boeing__filter_68._out(0) >> [boeing__database__loadi_111._in(1), boeing__alteryxselect_25._in(2)]
