from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "DQ_Macro",
    version = 1,
    auto_layout = False,
    params = Parameters(
      password_Query__Sheet1___143 = "''",
      jdbcUrl_DbFileOutput_18_182 = "''",
      DropDown_199 = "''",
      Question__User_Email_Option__Drop_Down_131 = "''",
      Question__Input_Reference_File__File_Browse_116 = "''",
      Question__Allow_Nulls__Drop_Down_199 = "''",
      workflow_name = "'DQ_Macro'",
      password_DbFileOutput_18_182 = "''",
      username_Query__Sheet1___144 = "''",
      username_DbFileOutput_18_182 = "''",
      username_Query__Sheet1___143 = "''",
      password_Query__Sheet1___144 = "''",
      Question__Input_Actual_File__File_Browse_119 = "''",
      Question__User_Email__Text_Box_133 = "''"
    )
)

with Pipeline(args) as pipeline:
    dq_macro__dbfileoutput_18_182 = Process(
        name = "DQ_Macro__DbFileOutput_18_182",
        properties = ModelTransform(modelName = "DQ_Macro__DbFileOutput_18_182"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    dq_macro__alteryxselect_147 = Process(
        name = "DQ_Macro__AlteryxSelect_147",
        properties = ModelTransform(modelName = "DQ_Macro__AlteryxSelect_147")
    )
    dq_macro__alteryxselect_19 = Process(
        name = "DQ_Macro__AlteryxSelect_19",
        properties = ModelTransform(modelName = "DQ_Macro__AlteryxSelect_19"),
        input_ports = ["in_0", "in_1"]
    )
    dq_macro__formula_23_1 = Process(
        name = "DQ_Macro__Formula_23_1",
        properties = ModelTransform(modelName = "DQ_Macro__Formula_23_1"),
        input_ports = ["in_0", "in_1"]
    )
    fieldinfo_15 = Process(
        name = "FieldInfo_15",
        properties = Script(
          scriptMethodHeader = "def Script(in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nfrom pyspark.sql import Row\nfrom pyspark.sql.types import StructField, StringType, IntegerType, StructType\n\nschema_info = []\nfor field in in0.schema.fields:\n    if field.metadata.get('size', None) is not None:\n        field_size = field.metadata['size']\n    else:\n        field_size = 4\n    schema_info.append((field.name, field.dataType.typeName(), field_size, \"\", \"\", None))\n\nschema = StructType([\n    StructField(\"Name\", StringType(), nullable=True),\n    StructField(\"variableType\", StringType(), nullable=True),\n    StructField(\"Size\", IntegerType(), nullable=True),\n    StructField(\"Description\", StringType(), nullable=True),\n    StructField(\"Source\", StringType(), nullable=True),\n    StructField(\"Scale\", IntegerType(), nullable=True)\n])\n\nout0 = spark.createDataFrame(\n    spark.sparkContext.parallelize([Row(*row) for row in schema_info]),\n    schema\n)\n\n"
        ),
        is_custom_output_schema = True
    )
    email_90 = Process(
        name = "Email_90",
        properties = Email(
          body = "Layout",
          bodyColumn = "Layout",
          subjectFromColumn = False,
          showCcBcc = False,
          to = "sample@gmail.com",
          toFromColumn = False,
          bodyFromColumn = True,
          toColumn = "samplegmail__com",
          ccFromColumn = False,
          bccFromColumn = False,
          connection = "transpiled_connection"
        )
    )
    dq_macro__filter_24 = Process(
        name = "DQ_Macro__Filter_24",
        properties = ModelTransform(modelName = "DQ_Macro__Filter_24")
    )
    dq_macro__alteryxselect_112 = Process(
        name = "DQ_Macro__AlteryxSelect_112",
        properties = ModelTransform(modelName = "DQ_Macro__AlteryxSelect_112"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    query__sheet1___144 = Process(
        name = "Query__Sheet1___144",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "",
              "username": "${username_Query__Sheet1___144}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Query__Sheet1___144"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`Sheet1$`")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/DQ_Macro/Query__Sheet1___144.yml")
        ),
        input_ports = None
    )
    dq_macro__alteryxselect_148 = Process(
        name = "DQ_Macro__AlteryxSelect_148",
        properties = ModelTransform(modelName = "DQ_Macro__AlteryxSelect_148")
    )
    dq_macro__portfoliocomposertext_173 = Process(
        name = "DQ_Macro__PortfolioComposerText_173",
        properties = ModelTransform(modelName = "DQ_Macro__PortfolioComposerText_173"),
        input_ports = None
    )
    dq_macro__alteryxselect_25 = Process(
        name = "DQ_Macro__AlteryxSelect_25",
        properties = ModelTransform(modelName = "DQ_Macro__AlteryxSelect_25")
    )
    fieldinfo_14 = Process(
        name = "FieldInfo_14",
        properties = Script(
          scriptMethodHeader = "def Script(in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nfrom pyspark.sql import Row\nfrom pyspark.sql.types import StructField, StringType, IntegerType, StructType\n\nschema_info = []\nfor field in in0.schema.fields:\n    if field.metadata.get('size', None) is not None:\n        field_size = field.metadata['size']\n    else:\n        field_size = 4\n    schema_info.append((field.name, field.dataType.typeName(), field_size, \"\", \"\", None))\n\nschema = StructType([\n    StructField(\"Name\", StringType(), nullable=True),\n    StructField(\"variableType\", StringType(), nullable=True),\n    StructField(\"Size\", IntegerType(), nullable=True),\n    StructField(\"Description\", StringType(), nullable=True),\n    StructField(\"Source\", StringType(), nullable=True),\n    StructField(\"Scale\", IntegerType(), nullable=True)\n])\n\nout0 = spark.createDataFrame(\n    spark.sparkContext.parallelize([Row(*row) for row in schema_info]),\n    schema\n)\n\n"
        ),
        is_custom_output_schema = True
    )
    dq_macro__portfoliocomposerrender_111 = Process(
        name = "DQ_Macro__PortfolioComposerRender_111",
        properties = ModelTransform(modelName = "DQ_Macro__PortfolioComposerRender_111")
    )
    textinput_151 = Process(
        name = "TextInput_151",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_151", sourceType = "Seed")
        ),
        input_ports = None
    )
    dq_macro__join_72_inner = Process(
        name = "DQ_Macro__Join_72_inner",
        properties = ModelTransform(modelName = "DQ_Macro__Join_72_inner"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    dq_macro__alteryxselect_20 = Process(
        name = "DQ_Macro__AlteryxSelect_20",
        properties = ModelTransform(modelName = "DQ_Macro__AlteryxSelect_20"),
        input_ports = ["in_0", "in_1"]
    )
    dq_macro__formula_163_0 = Process(
        name = "DQ_Macro__Formula_163_0",
        properties = ModelTransform(modelName = "DQ_Macro__Formula_163_0"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    query__sheet1___143 = Process(
        name = "Query__Sheet1___143",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "",
              "username": "${username_Query__Sheet1___143}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Query__Sheet1___143"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`Sheet1$`")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/DQ_Macro/Query__Sheet1___143.yml")
        ),
        input_ports = None
    )
    (
        query__sheet1___144._out(0)
        >> [dq_macro__alteryxselect_112._in(3), dq_macro__dbfileoutput_18_182._in(3),
              dq_macro__dbfileoutput_18_182._in(4), dq_macro__join_72_inner._in(0),
              dq_macro__formula_163_0._in(1), dq_macro__alteryxselect_148._in(0)]
    )
    dq_macro__alteryxselect_25._out(0) >> [dq_macro__alteryxselect_112._in(1), dq_macro__dbfileoutput_18_182._in(1)]
    (
        fieldinfo_15._out(0)
        >> [dq_macro__formula_23_1._in(0), dq_macro__alteryxselect_20._in(0), dq_macro__alteryxselect_19._in(0)]
    )
    dq_macro__formula_23_1._out(0) >> [dq_macro__filter_24._in(0), dq_macro__alteryxselect_25._in(0)]
    dq_macro__alteryxselect_112._out(0) >> [email_90._in(0), dq_macro__portfoliocomposerrender_111._in(0)]
    (
        fieldinfo_14._out(0)
        >> [dq_macro__formula_23_1._in(1), dq_macro__alteryxselect_20._in(1), dq_macro__alteryxselect_19._in(1)]
    )
    dq_macro__filter_24._out(0) >> [dq_macro__alteryxselect_112._in(4), dq_macro__join_72_inner._in(2)]
    dq_macro__formula_163_0._out(0) >> [dq_macro__alteryxselect_112._in(6), dq_macro__dbfileoutput_18_182._in(6)]
    dq_macro__alteryxselect_148 >> fieldinfo_15
    query__sheet1___143._out(0) >> [dq_macro__formula_163_0._in(0), dq_macro__alteryxselect_147._in(0)]
    dq_macro__alteryxselect_20._out(0) >> [dq_macro__alteryxselect_112._in(0), dq_macro__dbfileoutput_18_182._in(0)]
    dq_macro__join_72_inner._out(0) >> [dq_macro__alteryxselect_112._in(5), dq_macro__dbfileoutput_18_182._in(5)]
    dq_macro__alteryxselect_19._out(0) >> [dq_macro__alteryxselect_112._in(2), dq_macro__dbfileoutput_18_182._in(2)]
    textinput_151 >> dq_macro__formula_163_0._in(2)
    dq_macro__alteryxselect_147 >> fieldinfo_14
