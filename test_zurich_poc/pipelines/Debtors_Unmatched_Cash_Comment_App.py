from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Debtors_Unmatched_Cash_Comment_App",
    version = 1,
    auto_layout = False,
    params = Parameters(
      Question__variable__cloudUserId = "''",
      jdbcUrl_aka_SnowflakePR_12 = "''",
      Question__Text_Box_92 = "''",
      Question__Radio_Button_87 = "''",
      password_aka_SnowflakePR_12 = "''",
      Question__Radio_Button_32 = "''",
      Question__File_Browse_57 = "''",
      Question__Drop_Down_21 = "''",
      username_aka_SnowflakePR_12 = "''",
      Question__Drop_Down_65 = "''",
      workflow_name = "'Debtors_Unmatched_Cash_Comment_App'",
      Text_Box_7 = "'AA'",
      variable__cloudUserId = "'AA'",
      Question__Text_Box_5 = "''",
      Question__Radio_Button_121 = "''",
      password_aka_SnowflakePR_13 = "''",
      Question__Text_Box_7 = "''",
      Text_Box_94 = "'AA'",
      Drop_Down_65 = "'AA'",
      Question__Text_Box_94 = "''",
      Drop_Down_21 = "''",
      username_aka_SnowflakePR_13 = "''"
    )
)

with Pipeline(args) as pipeline:
    debtors_unmatched_cash_comment_app__crosstab_67 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__CrossTab_67",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__CrossTab_67")
    )
    debtors_unmatched_cash_comment_app__aka_snowflakepr_12 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__aka_SnowflakePR_12",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__aka_SnowflakePR_12")
    )
    textinput_66 = Process(
        name = "TextInput_66",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_66", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_89 = Process(
        name = "TextInput_89",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_89", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_22 = Process(
        name = "TextInput_22",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_22", sourceType = "Seed")
        ),
        input_ports = None
    )
    bulk_comments_x_56 = Process(
        name = "BULK_COMMENTS_x_56",
        properties = SFTPSource(
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
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
          properties = SFTPSource.SFTPSourceInternal(
            filePath = "\\\\ms.zurich.com.au\\DFS\\AlteryX\\Prod\\Finance\\Debtors_Unmatched_Cash\\Input\\BULK_COMMENTS.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/Debtors_Unmatched_Cash_Comment_App/BULK_COMMENTS_x_56.yml"
          )
        ),
        input_ports = None
    )
    debtors_unmatched_cash_comment_app__union_55 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Union_55",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Union_55"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    debtors_unmatched_cash_comment_app__filter_26 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Filter_26",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Filter_26")
    )
    debtors_unmatched_cash_comment_app__filter_107 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Filter_107",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Filter_107")
    )
    aka_snowflakepr_13 = Process(
        name = "aka_SnowflakePR_13",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:66625a08023cf31e5dc6126a",
              "username": "${username_aka_SnowflakePR_13}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_SnowflakePR_13"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select * \nfrom PRD_BDS.FINANCE_GENERAL.MRT_DIM_DEBTORS_TRACKER_ROLES\nwhere VALID_TO is null"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Debtors_Unmatched_Cash_Comment_App/aka_SnowflakePR_13.yml"
          )
        ),
        input_ports = None
    )
    debtors_unmatched_cash_comment_app__crosstab_24 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__CrossTab_24",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__CrossTab_24")
    )
    test_103 = Process(
        name = "Test_103",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "assert ( in0.count()  == 1 )"
        )
    )
    test_112 = Process(
        name = "Test_112",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "assert ( in0.count()  == 1 )"
        )
    )
    textinput_1 = Process(
        name = "TextInput_1",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_1", sourceType = "Seed")
        ),
        input_ports = None
    )
    debtors_unmatched_cash_comment_app__filter_111 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Filter_111",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Filter_111")
    )
    debtors_unmatched_cash_comment_app__unique_118 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Unique_118",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Unique_118")
    )
    test_29 = Process(
        name = "Test_29",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "assert ( in0.count()  == 1 )"
        )
    )
    test_63 = Process(
        name = "Test_63",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "assert ( in0.filter((~isnull(col(\"COMMENTS\"))) & (~(isnull(col(\"COMMENTS\")) | (length(col(\"COMMENTS\")) == lit(0)))) & ((col(\"COMMENTS\") != lit(\"\")) | isnull(col(\"COMMENTS\"))) & (length(col(\"COMMENTS\")) > lit(5))).count() > 0 )"
        )
    )
    (
        debtors_unmatched_cash_comment_app__filter_107._out(0)
        >> [test_103._in(0), debtors_unmatched_cash_comment_app__union_55._in(1)]
    )
    bulk_comments_x_56 >> debtors_unmatched_cash_comment_app__union_55._in(5)
    (
        debtors_unmatched_cash_comment_app__filter_26._out(0)
        >> [test_29._in(0), debtors_unmatched_cash_comment_app__union_55._in(0)]
    )
    textinput_66 >> debtors_unmatched_cash_comment_app__crosstab_67
    textinput_1 >> debtors_unmatched_cash_comment_app__union_55._in(4)
    (
        debtors_unmatched_cash_comment_app__filter_111._out(0)
        >> [debtors_unmatched_cash_comment_app__union_55._in(2), debtors_unmatched_cash_comment_app__unique_118._in(0)]
    )
    debtors_unmatched_cash_comment_app__unique_118 >> test_112
    textinput_89 >> debtors_unmatched_cash_comment_app__union_55._in(3)
    (
        debtors_unmatched_cash_comment_app__union_55._out(0)
        >> [test_63._in(0), debtors_unmatched_cash_comment_app__aka_snowflakepr_12._in(0)]
    )
    (
        aka_snowflakepr_13._out(0)
        >> [debtors_unmatched_cash_comment_app__filter_107._in(0),
              debtors_unmatched_cash_comment_app__filter_111._in(0),
              debtors_unmatched_cash_comment_app__filter_26._in(0)]
    )
    textinput_22 >> debtors_unmatched_cash_comment_app__crosstab_24
