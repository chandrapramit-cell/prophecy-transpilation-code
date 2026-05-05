from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "alteryx_all_test_sql",
    version = 1,
    auto_layout = False,
    params = Parameters(WORKFLOW_NAME = "'alteryx_all_test_sql'")
)

with Pipeline(args) as pipeline:
    macro_459 = Process(
        name = "Macro_459",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "\n#Failed to parse Macro. Please upload the file Macro2_doubleport.yxmc to resolve it.\n#<Node ToolID=\"459\">\n#      <GuiSettings>\n#        <Position x=\"486\" y=\"5622\"/>\n#      </GuiSettings>\n#      <Properties>\n#        <Configuration/>\n#        <Annotation DisplayMode=\"0\">\n#          <Name/>\n#          <DefaultAnnotationText/>\n#          <Left value=\"False\"/>\n#        </Annotation>\n#        <Dependencies>\n#          <Implicit/>\n#        </Dependencies>\n#      </Properties>\n#      <EngineSettings Macro=\"macros\\Macro2_doubleport.yxmc\"/>\n#    </Node>\n"
        ),
        input_ports = None,
        output_ports = 2,
        is_custom_output_schema = True
    )
    makecolumns_32 = Process(
        name = "MakeColumns_32",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport pandas as pd\n\ndf = in0.copy()\ngroup_cols = [\"_group_id\"]\npivot_col = \"_position_id\"\nagg_dict = {\"Units\": \"max\", \"_position_id\": \"max\", \"UnitCost\": \"max\", \"_sequence_id\": \"max\", \"_total_records\": \"max\", \"Store Number\": \"max\"}\ncol_to_alias = {\"Units\": \"Units\", \"_position_id\": \"_position_id\", \"UnitCost\": \"UnitCost\", \"_sequence_id\": \"_sequence_id\", \"_total_records\": \"_total_records\", \"Store Number\": \"Store Number\"}\n\n\ndf_pivoted = pd.pivot_table(\n    df,\n    index=group_cols,\n    columns=pivot_col if pivot_col else None,\n    values=list(agg_dict.keys()),\n    aggfunc=agg_dict,\n)\n\nif col_to_alias:\n    if isinstance(df_pivoted.columns, pd.MultiIndex):\n        new_cols = []\n        for i in range(len(df_pivoted.columns)):\n            pivot_val = df_pivoted.columns.get_level_values(-1)[i]\n            val_col = df_pivoted.columns.get_level_values(0)[i]\n            alias = col_to_alias.get(val_col, val_col)\n            new_cols.append(f\"{pivot_val}_{alias}\")\n        df_pivoted.columns = new_cols\n    else:\n        alias = list(col_to_alias.values())[0] if col_to_alias else \"\"\n        df_pivoted = df_pivoted.rename(columns={c: f\"{c}_{alias}\" for c in df_pivoted.columns})\n\ndf_pivoted = df_pivoted.reset_index()\n\n\nreturn df_pivoted\n"
        ),
        is_custom_output_schema = True
    )
    makecolumns_33 = Process(
        name = "MakeColumns_33",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport pandas as pd\n\ndf = in0.copy()\ngroup_cols = [\"_group_id\"]\npivot_col = \"_position_id\"\nagg_dict = {\"Units\": \"max\", \"_position_id\": \"max\", \"UnitCost\": \"max\", \"_sequence_id\": \"max\", \"_total_records\": \"max\", \"Store Number\": \"max\"}\ncol_to_alias = {\"Units\": \"Units\", \"_position_id\": \"_position_id\", \"UnitCost\": \"UnitCost\", \"_sequence_id\": \"_sequence_id\", \"_total_records\": \"_total_records\", \"Store Number\": \"Store Number\"}\n\n\ndf_pivoted = pd.pivot_table(\n    df,\n    index=group_cols,\n    columns=pivot_col if pivot_col else None,\n    values=list(agg_dict.keys()),\n    aggfunc=agg_dict,\n)\n\nif col_to_alias:\n    if isinstance(df_pivoted.columns, pd.MultiIndex):\n        new_cols = []\n        for i in range(len(df_pivoted.columns)):\n            pivot_val = df_pivoted.columns.get_level_values(-1)[i]\n            val_col = df_pivoted.columns.get_level_values(0)[i]\n            alias = col_to_alias.get(val_col, val_col)\n            new_cols.append(f\"{pivot_val}_{alias}\")\n        df_pivoted.columns = new_cols\n    else:\n        alias = list(col_to_alias.values())[0] if col_to_alias else \"\"\n        df_pivoted = df_pivoted.rename(columns={c: f\"{c}_{alias}\" for c in df_pivoted.columns})\n\ndf_pivoted = df_pivoted.reset_index()\n\n\nreturn df_pivoted\n"
        ),
        is_custom_output_schema = True
    )
    makecolumns_34 = Process(
        name = "MakeColumns_34",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport pandas as pd\n\ndf = in0.copy()\ngroup_cols = [\"Store Number\", \"_group_id\"]\npivot_col = \"_position_id\"\nagg_dict = {\"Units\": \"max\", \"_position_id\": \"max\", \"UnitCost\": \"max\", \"_sequence_id\": \"max\", \"_total_records\": \"max\"}\ncol_to_alias = {\"Units\": \"Units\", \"_position_id\": \"_position_id\", \"UnitCost\": \"UnitCost\", \"_sequence_id\": \"_sequence_id\", \"_total_records\": \"_total_records\"}\n\n\ndf_pivoted = pd.pivot_table(\n    df,\n    index=group_cols,\n    columns=pivot_col if pivot_col else None,\n    values=list(agg_dict.keys()),\n    aggfunc=agg_dict,\n)\n\nif col_to_alias:\n    if isinstance(df_pivoted.columns, pd.MultiIndex):\n        new_cols = []\n        for i in range(len(df_pivoted.columns)):\n            pivot_val = df_pivoted.columns.get_level_values(-1)[i]\n            val_col = df_pivoted.columns.get_level_values(0)[i]\n            alias = col_to_alias.get(val_col, val_col)\n            new_cols.append(f\"{pivot_val}_{alias}\")\n        df_pivoted.columns = new_cols\n    else:\n        alias = list(col_to_alias.values())[0] if col_to_alias else \"\"\n        df_pivoted = df_pivoted.rename(columns={c: f\"{c}_{alias}\" for c in df_pivoted.columns})\n\ndf_pivoted = df_pivoted.reset_index()\n\n\nreturn df_pivoted\n"
        ),
        is_custom_output_schema = True
    )
    makecolumns_35 = Process(
        name = "MakeColumns_35",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport pandas as pd\n\ndf = in0.copy()\ngroup_cols = [\"Store Number\", \"_group_id\"]\npivot_col = \"_position_id\"\nagg_dict = {\"Units\": \"max\", \"_position_id\": \"max\", \"UnitCost\": \"max\", \"_sequence_id\": \"max\", \"_total_records\": \"max\"}\ncol_to_alias = {\"Units\": \"Units\", \"_position_id\": \"_position_id\", \"UnitCost\": \"UnitCost\", \"_sequence_id\": \"_sequence_id\", \"_total_records\": \"_total_records\"}\n\n\ndf_pivoted = pd.pivot_table(\n    df,\n    index=group_cols,\n    columns=pivot_col if pivot_col else None,\n    values=list(agg_dict.keys()),\n    aggfunc=agg_dict,\n)\n\nif col_to_alias:\n    if isinstance(df_pivoted.columns, pd.MultiIndex):\n        new_cols = []\n        for i in range(len(df_pivoted.columns)):\n            pivot_val = df_pivoted.columns.get_level_values(-1)[i]\n            val_col = df_pivoted.columns.get_level_values(0)[i]\n            alias = col_to_alias.get(val_col, val_col)\n            new_cols.append(f\"{pivot_val}_{alias}\")\n        df_pivoted.columns = new_cols\n    else:\n        alias = list(col_to_alias.values())[0] if col_to_alias else \"\"\n        df_pivoted = df_pivoted.rename(columns={c: f\"{c}_{alias}\" for c in df_pivoted.columns})\n\ndf_pivoted = df_pivoted.reset_index()\n\n\nreturn df_pivoted\n"
        ),
        is_custom_output_schema = True
    )
    makecolumns_36 = Process(
        name = "MakeColumns_36",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport pandas as pd\n\ndf = in0.copy()\ngroup_cols = [\"Store Number\", \"Units\", \"_group_id\"]\npivot_col = \"_position_id\"\nagg_dict = {\"_position_id\": \"max\", \"UnitCost\": \"max\", \"_sequence_id\": \"max\", \"_total_records\": \"max\"}\ncol_to_alias = {\"_position_id\": \"_position_id\", \"UnitCost\": \"UnitCost\", \"_sequence_id\": \"_sequence_id\", \"_total_records\": \"_total_records\"}\n\n\ndf_pivoted = pd.pivot_table(\n    df,\n    index=group_cols,\n    columns=pivot_col if pivot_col else None,\n    values=list(agg_dict.keys()),\n    aggfunc=agg_dict,\n)\n\nif col_to_alias:\n    if isinstance(df_pivoted.columns, pd.MultiIndex):\n        new_cols = []\n        for i in range(len(df_pivoted.columns)):\n            pivot_val = df_pivoted.columns.get_level_values(-1)[i]\n            val_col = df_pivoted.columns.get_level_values(0)[i]\n            alias = col_to_alias.get(val_col, val_col)\n            new_cols.append(f\"{pivot_val}_{alias}\")\n        df_pivoted.columns = new_cols\n    else:\n        alias = list(col_to_alias.values())[0] if col_to_alias else \"\"\n        df_pivoted = df_pivoted.rename(columns={c: f\"{c}_{alias}\" for c in df_pivoted.columns})\n\ndf_pivoted = df_pivoted.reset_index()\n\n\nreturn df_pivoted\n"
        ),
        is_custom_output_schema = True
    )
    makecolumns_37 = Process(
        name = "MakeColumns_37",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport pandas as pd\n\ndf = in0.copy()\ngroup_cols = [\"Store Number\", \"UnitCost\", \"_group_id\"]\npivot_col = \"_position_id\"\nagg_dict = {\"Units\": \"max\", \"_position_id\": \"max\", \"_sequence_id\": \"max\", \"_total_records\": \"max\"}\ncol_to_alias = {\"Units\": \"Units\", \"_position_id\": \"_position_id\", \"_sequence_id\": \"_sequence_id\", \"_total_records\": \"_total_records\"}\n\n\ndf_pivoted = pd.pivot_table(\n    df,\n    index=group_cols,\n    columns=pivot_col if pivot_col else None,\n    values=list(agg_dict.keys()),\n    aggfunc=agg_dict,\n)\n\nif col_to_alias:\n    if isinstance(df_pivoted.columns, pd.MultiIndex):\n        new_cols = []\n        for i in range(len(df_pivoted.columns)):\n            pivot_val = df_pivoted.columns.get_level_values(-1)[i]\n            val_col = df_pivoted.columns.get_level_values(0)[i]\n            alias = col_to_alias.get(val_col, val_col)\n            new_cols.append(f\"{pivot_val}_{alias}\")\n        df_pivoted.columns = new_cols\n    else:\n        alias = list(col_to_alias.values())[0] if col_to_alias else \"\"\n        df_pivoted = df_pivoted.rename(columns={c: f\"{c}_{alias}\" for c in df_pivoted.columns})\n\ndf_pivoted = df_pivoted.reset_index()\n\n\nreturn df_pivoted\n"
        ),
        is_custom_output_schema = True
    )
    textinput_106 = Process(
        name = "TextInput_106",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_106", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_11 = Process(
        name = "TextInput_11",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_11", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_110 = Process(
        name = "TextInput_110",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_110", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_112 = Process(
        name = "TextInput_112",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_112", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_116 = Process(
        name = "TextInput_116",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_116", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_118 = Process(
        name = "TextInput_118",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_118", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_1300 = Process(
        name = "TextInput_1300",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_1300", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_135 = Process(
        name = "TextInput_135",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_135", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_137 = Process(
        name = "TextInput_137",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_137", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_138 = Process(
        name = "TextInput_138",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_138", sourceType = "Seed")
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
    textinput_24 = Process(
        name = "TextInput_24",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_24", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_25 = Process(
        name = "TextInput_25",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_25", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_262 = Process(
        name = "TextInput_262",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_262", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_270 = Process(
        name = "TextInput_270",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_270", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_29 = Process(
        name = "TextInput_29",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_29", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_290 = Process(
        name = "TextInput_290",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_290", sourceType = "Seed")
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
    textinput_51 = Process(
        name = "TextInput_51",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_51", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_53 = Process(
        name = "TextInput_53",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_53", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_61 = Process(
        name = "TextInput_61",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_61", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_66 = Process(
        name = "TextInput_66",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_66", sourceType = "Seed")
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
    alteryx_all_tes_284 = Process(
        name = "alteryx_all_tes_284",
        properties = SFTPTarget(
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
            filePath = "C:\\Users\\prophecy\\Downloads\\alteryx_files\\Demo_Aug1\\Output_Data\\alteryx_all_test_output.csv"
          )
        )
    )
    alteryx_all_test_sql__makecolumns_32_groupandposition_0 = Process(
        name = "alteryx_all_test_sql__MakeColumns_32_groupAndPosition_0",
        properties = ModelTransform(modelName = "alteryx_all_test_sql__MakeColumns_32_groupAndPosition_0")
    )
    alteryx_all_test_sql__makecolumns_33_groupandposition_0 = Process(
        name = "alteryx_all_test_sql__MakeColumns_33_groupAndPosition_0",
        properties = ModelTransform(modelName = "alteryx_all_test_sql__MakeColumns_33_groupAndPosition_0")
    )
    alteryx_all_test_sql__makecolumns_34_groupandposition_0 = Process(
        name = "alteryx_all_test_sql__MakeColumns_34_groupAndPosition_0",
        properties = ModelTransform(modelName = "alteryx_all_test_sql__MakeColumns_34_groupAndPosition_0")
    )
    alteryx_all_test_sql__makecolumns_35_groupandposition_0 = Process(
        name = "alteryx_all_test_sql__MakeColumns_35_groupAndPosition_0",
        properties = ModelTransform(modelName = "alteryx_all_test_sql__MakeColumns_35_groupAndPosition_0")
    )
    alteryx_all_test_sql__makecolumns_36_groupandposition_0 = Process(
        name = "alteryx_all_test_sql__MakeColumns_36_groupAndPosition_0",
        properties = ModelTransform(modelName = "alteryx_all_test_sql__MakeColumns_36_groupAndPosition_0")
    )
    alteryx_all_test_sql__makecolumns_37_groupandposition_0 = Process(
        name = "alteryx_all_test_sql__MakeColumns_37_groupAndPosition_0",
        properties = ModelTransform(modelName = "alteryx_all_test_sql__MakeColumns_37_groupAndPosition_0")
    )
    alteryx_all_test_sql__textinput_31_cast = Process(
        name = "alteryx_all_test_sql__TextInput_31_cast",
        properties = ModelTransform(modelName = "alteryx_all_test_sql__TextInput_31_cast")
    )
    alteryx_all_test_sql__union_283 = Process(
        name = "alteryx_all_test_sql__Union_283",
        properties = ModelTransform(modelName = "alteryx_all_test_sql__Union_283"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17", "in_18", "in_19", "in_20", "in_21", "in_22", "in_23",
         "in_24", "in_25", "in_26", "in_27", "in_28", "in_29"]
    )
    makecolumns_34 >> alteryx_all_test_sql__union_283._in(24)
    textinput_11 >> alteryx_all_test_sql__union_283._in(1)
    textinput_51 >> alteryx_all_test_sql__union_283._in(6)
    textinput_7 >> alteryx_all_test_sql__union_283._in(0)
    textinput_116 >> alteryx_all_test_sql__union_283._in(13)
    macro_459._out(0) >> alteryx_all_test_sql__union_283._in(28)
    textinput_262 >> alteryx_all_test_sql__union_283._in(19)
    macro_459._out(1) >> alteryx_all_test_sql__union_283._in(29)
    alteryx_all_test_sql__makecolumns_35_groupandposition_0 >> makecolumns_35
    textinput_29 >> alteryx_all_test_sql__union_283._in(5)
    textinput_61 >> alteryx_all_test_sql__union_283._in(8)
    textinput_110 >> alteryx_all_test_sql__union_283._in(11)
    textinput_270 >> alteryx_all_test_sql__union_283._in(20)
    textinput_290 >> alteryx_all_test_sql__union_283._in(21)
    textinput_138 >> alteryx_all_test_sql__union_283._in(17)
    makecolumns_37 >> alteryx_all_test_sql__union_283._in(22)
    textinput_106 >> alteryx_all_test_sql__union_283._in(10)
    alteryx_all_test_sql__makecolumns_33_groupandposition_0 >> makecolumns_33
    makecolumns_36 >> alteryx_all_test_sql__union_283._in(26)
    alteryx_all_test_sql__makecolumns_36_groupandposition_0 >> makecolumns_36
    textinput_66 >> alteryx_all_test_sql__union_283._in(9)
    textinput_53 >> alteryx_all_test_sql__union_283._in(7)
    alteryx_all_test_sql__makecolumns_37_groupandposition_0 >> makecolumns_37
    textinput_1300 >> alteryx_all_test_sql__union_283._in(2)
    makecolumns_33 >> alteryx_all_test_sql__union_283._in(23)
    alteryx_all_test_sql__union_283 >> alteryx_all_tes_284
    alteryx_all_test_sql__makecolumns_32_groupandposition_0 >> makecolumns_32
    (
        alteryx_all_test_sql__textinput_31_cast._out(0)
        >> [alteryx_all_test_sql__makecolumns_32_groupandposition_0._in(0),
              alteryx_all_test_sql__makecolumns_33_groupandposition_0._in(0),
              alteryx_all_test_sql__makecolumns_34_groupandposition_0._in(0),
              alteryx_all_test_sql__makecolumns_35_groupandposition_0._in(0),
              alteryx_all_test_sql__makecolumns_36_groupandposition_0._in(0),
              alteryx_all_test_sql__makecolumns_37_groupandposition_0._in(0)]
    )
    textinput_25 >> alteryx_all_test_sql__union_283._in(4)
    alteryx_all_test_sql__makecolumns_34_groupandposition_0 >> makecolumns_34
    textinput_118 >> alteryx_all_test_sql__union_283._in(14)
    textinput_137 >> alteryx_all_test_sql__union_283._in(16)
    textinput_135 >> alteryx_all_test_sql__union_283._in(15)
    textinput_112 >> alteryx_all_test_sql__union_283._in(12)
    makecolumns_32 >> alteryx_all_test_sql__union_283._in(27)
    textinput_159 >> alteryx_all_test_sql__union_283._in(18)
    makecolumns_35 >> alteryx_all_test_sql__union_283._in(25)
    textinput_31 >> alteryx_all_test_sql__textinput_31_cast
    textinput_24 >> alteryx_all_test_sql__union_283._in(3)
