from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Challenge_419_solution_file_1_",
    version = 1,
    auto_layout = False,
    params = Parameters(WORKFLOW_NAME = "'Challenge_419_solution_file_1_'")
)

with Pipeline(args) as pipeline:

    with visual_group("Task1"):
        pass

    with visual_group("Task2"):
        pass

    challenge_419_solution_file_1___alteryxselect_42 = Process(
        name = "Challenge_419_solution_file_1___AlteryxSelect_42",
        properties = ModelTransform(modelName = "Challenge_419_solution_file_1___AlteryxSelect_42")
    )
    challenge_419_solution_file_1___formula_22_0 = Process(
        name = "Challenge_419_solution_file_1___Formula_22_0",
        properties = ModelTransform(modelName = "Challenge_419_solution_file_1___Formula_22_0")
    )
    challenge_419_solution_file_1___multirowformula_19_0 = Process(
        name = "Challenge_419_solution_file_1___MultiRowFormula_19_0",
        properties = ModelTransform(modelName = "Challenge_419_solution_file_1___MultiRowFormula_19_0"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    challenge_419_solution_file_1___textinput_31_cast = Process(
        name = "Challenge_419_solution_file_1___TextInput_31_cast",
        properties = ModelTransform(modelName = "Challenge_419_solution_file_1___TextInput_31_cast")
    )
    challenge_419_solution_file_1___textinput_32_cast = Process(
        name = "Challenge_419_solution_file_1___TextInput_32_cast",
        properties = ModelTransform(modelName = "Challenge_419_solution_file_1___TextInput_32_cast")
    )
    challenge_419_solution_file_1___textinput_33_cast = Process(
        name = "Challenge_419_solution_file_1___TextInput_33_cast",
        properties = ModelTransform(modelName = "Challenge_419_solution_file_1___TextInput_33_cast")
    )
    challenge_419_solution_file_1___textinput_48_cast = Process(
        name = "Challenge_419_solution_file_1___TextInput_48_cast",
        properties = ModelTransform(modelName = "Challenge_419_solution_file_1___TextInput_48_cast")
    )
    challenge_419_solution_file_1___textinput_49_cast = Process(
        name = "Challenge_419_solution_file_1___TextInput_49_cast",
        properties = ModelTransform(modelName = "Challenge_419_solution_file_1___TextInput_49_cast")
    )
    textinput_2 = Process(
        name = "TextInput_2",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_2", sourceType = "Seed")
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
    textinput_31 = Process(
        name = "TextInput_31",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_31", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_32 = Process(
        name = "TextInput_32",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_32", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_33 = Process(
        name = "TextInput_33",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_33", sourceType = "Seed")
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
    textinput_49 = Process(
        name = "TextInput_49",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_49", sourceType = "Seed")
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
    textinput_33 >> challenge_419_solution_file_1___textinput_33_cast
    (
        challenge_419_solution_file_1___multirowformula_19_0._out(0)
        >> [challenge_419_solution_file_1___formula_22_0._in(0),
              challenge_419_solution_file_1___alteryxselect_42._in(0)]
    )
    textinput_6 >> challenge_419_solution_file_1___multirowformula_19_0._in(2)
    textinput_2 >> challenge_419_solution_file_1___multirowformula_19_0._in(0)
    textinput_31 >> challenge_419_solution_file_1___textinput_31_cast
    textinput_48 >> challenge_419_solution_file_1___textinput_48_cast
    textinput_32 >> challenge_419_solution_file_1___textinput_32_cast
    textinput_3 >> challenge_419_solution_file_1___multirowformula_19_0._in(1)
    textinput_49 >> challenge_419_solution_file_1___textinput_49_cast
