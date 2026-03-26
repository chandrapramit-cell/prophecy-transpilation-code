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
      Question__Text_Box_5 = "''",
      Question__Radio_Button_121 = "''",
      password_aka_SnowflakePR_13 = "''",
      Question__Text_Box_7 = "''",
      Question__Text_Box_94 = "''",
      username_aka_SnowflakePR_13 = "''"
    )
)

with Pipeline(args) as pipeline:

    with visual_group("CLIENTIDDATA"):
        pass

    with visual_group("CommentaryControl"):
        pass

    with visual_group("GI"):
        pass

    with visual_group("OUTPUT"):
        pass

    with visual_group("RI"):
        pass

    debtors_unmatched_cash_comment_app__crosstab_24 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__CrossTab_24",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__CrossTab_24")
    )
    debtors_unmatched_cash_comment_app__crosstab_67 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__CrossTab_67",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__CrossTab_67")
    )
    debtors_unmatched_cash_comment_app__filter_107 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Filter_107",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Filter_107")
    )
    debtors_unmatched_cash_comment_app__filter_107_reject = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Filter_107_reject",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Filter_107_reject")
    )
    debtors_unmatched_cash_comment_app__filter_26 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Filter_26",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Filter_26")
    )
    debtors_unmatched_cash_comment_app__filter_26_reject = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Filter_26_reject",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Filter_26_reject")
    )
    debtors_unmatched_cash_comment_app__union_55 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Union_55",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Union_55"),
        input_ports = None
    )
    debtors_unmatched_cash_comment_app__unique_118 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__Unique_118",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__Unique_118")
    )
    debtors_unmatched_cash_comment_app__aka_snowflakepr_12 = Process(
        name = "Debtors_Unmatched_Cash_Comment_App__aka_SnowflakePR_12",
        properties = ModelTransform(modelName = "Debtors_Unmatched_Cash_Comment_App__aka_SnowflakePR_12")
    )
    test_63 = Process(
        name = "Test_63",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "assert ( in0.filter((~isnull(col(\"COMMENTS\"))) & (~(isnull(col(\"COMMENTS\")) | (length(col(\"COMMENTS\")) == lit(0)))) & ((col(\"COMMENTS\") != lit(\"\")) | isnull(col(\"COMMENTS\"))) & (length(col(\"COMMENTS\")) > lit(5))).count() > 0 )"
        )
    )
    textinput_22 = Process(
        name = "TextInput_22",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_22", sourceType = "Seed")
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
    comment_1 = Annotation(name = "Comment_1", comment = "")
    comment_2 = Annotation(name = "Comment_2", comment = "")
    (
        aka_snowflakepr_13._out(0)
        >> [debtors_unmatched_cash_comment_app__filter_107._in(0),
              debtors_unmatched_cash_comment_app__filter_107_reject._in(0),
              debtors_unmatched_cash_comment_app__unique_118._in(0),
              debtors_unmatched_cash_comment_app__filter_26_reject._in(0),
              debtors_unmatched_cash_comment_app__filter_26._in(0)]
    )
    textinput_22 >> debtors_unmatched_cash_comment_app__crosstab_24
    textinput_66 >> debtors_unmatched_cash_comment_app__crosstab_67
    (
        debtors_unmatched_cash_comment_app__union_55._out(0)
        >> [test_63._in(0), debtors_unmatched_cash_comment_app__aka_snowflakepr_12._in(0)]
    )
