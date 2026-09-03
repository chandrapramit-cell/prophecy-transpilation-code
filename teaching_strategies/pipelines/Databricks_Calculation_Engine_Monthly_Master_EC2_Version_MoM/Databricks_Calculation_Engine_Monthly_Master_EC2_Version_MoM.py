from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM",
    version = 1,
    auto_layout = False,
    params = Parameters(
      workflow_name = "'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM'",
      User__TestingCustomer = "'02MNDOE'",
      User__Current_Period = "'2026-07-31'"
    )
)

with Pipeline(args) as pipeline:
    batchmacrooutputboundarytruncate_macro_3118 = Process(
        name = "BatchMacroOutputBoundaryTruncate_Macro_3118",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "spark.sql(\"TRUNCATE TABLE table_3118_Loop_macro_op\")\nspark.sql(\"TRUNCATE TABLE table_3118_Exit_macro_op\")\nout0 = in0"
        ),
        is_custom_output_schema = True
    )
    batchmacrooutputboundarytruncate_macro_3124 = Process(
        name = "BatchMacroOutputBoundaryTruncate_Macro_3124",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "spark.sql(\"TRUNCATE TABLE table_3124_Loop_macro_op\")\nspark.sql(\"TRUNCATE TABLE table_3124_Exit_macro_op\")\nout0 = in0"
        ),
        is_custom_output_schema = True
    )
    combinedopportu_2436 = Process(
        name = "CombinedOpportu_2436",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Combined Opportunities Adjustments.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation(includeSheetNameColumn = False)
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/CombinedOpportu_2436.yml",
            sheetName = "Sheet1",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    dsn_databricks__3331 = Process(
        name = "DSN_Databricks__3331",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "odbc:DSN=Databricks;UID=token;PWD=__EncPwd1__",
              "username": "${username_DSN_Databricks__3331}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "DSN_Databricks__3331"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT\n    MAS90_CUSTOMER_NUMBER AS \"Order: Account Name: Mas90 Customer Number\",\n    ACCOUNT_NAME AS \"Order: Account Name: Account Name\",\n    ORDER_SUBSCRIPTION_TERM AS \"Order: Subscription Term\",\n    ORDER_START_DATE AS \"Order: Start Date\",\n    ORDER_END_DATE AS \"Order: End Date (Calculated)\",\n    OPPORTUNITY_NAME AS \"Order: Opportunity: Opportunity Name\",\n    QUOTE_NUMBER AS \"Order: Quote Number\",\n    ORDER_NUMBER AS \"Order: Order\",\n    SALES_ORDER_NUMBER AS \"Order: Sales Order Number\",\n    ORDER_ACTIVATED_DATE AS \"Order: Activated Date\",\n    PRODUCT_CODE AS \"Product Code_Boomi\",\n    PRODUCT_NAME AS \"Product Name\",\n    LIST_PRICE AS \"List Price\",\n    UNIT_PRICE AS \"Unit Price\",\n    QUANTITY AS \"Quantity\",\n    TOTAL_PRICE AS \"Total Price (new)\",\n    OPPTY_RENEWED_CONTRACT_NUMBER AS \"Order: Opportunity: Renewed Contract: Order: Contract Number\",\n    OPPTY_RENEWED_ORDER_NUMBER AS \"Order: Opportunity: Renewed Contract: Order: Order\",\n    OPPTY_RENEWED_SALES_ORDER_NUMBER AS \"Order: Opportunity: Renewed Contract: Order: Sales Order Number\",\n    Actual_Closed_Date AS \"Order: Opportunity: Actual Closed Date\",\n    Business_Subtype AS \"Order: Business Subtype\"\n FROM `report`.`arr_alteryx_order_product_vw` LIMIT 1000000"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/DSN_Databricks__3331.yml"
          )
        ),
        input_ports = None
    )
    dsn_databricks__3336 = Process(
        name = "DSN_Databricks__3336",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "odbc:DSN=Databricks;UID=token;PWD=__EncPwd1__",
              "username": "${username_DSN_Databricks__3336}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "DSN_Databricks__3336"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`report`.`arr_alteryx_opportunity_product_vw`")
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/DSN_Databricks__3336.yml"
          )
        ),
        input_ports = None
    )
    dsn_databricks__3338 = Process(
        name = "DSN_Databricks__3338",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "odbc:DSN=Databricks;UID=token;PWD=__EncPwd1__",
              "username": "${username_DSN_Databricks__3338}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "DSN_Databricks__3338"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`report`.`arr_alteryx_reduction_line_vw`")
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/DSN_Databricks__3338.yml"
          )
        ),
        input_ports = None
    )
    dsn_databricks__3340 = Process(
        name = "DSN_Databricks__3340",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "odbc:DSN=Databricks;UID=token;PWD=__EncPwd1__",
              "username": "${username_DSN_Databricks__3340}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "DSN_Databricks__3340"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`report`.`arr_alteryx_customer_merge_vw`")
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/DSN_Databricks__3340.yml"
          )
        ),
        input_ports = None
    )
    dsn_databricks__3343 = Process(
        name = "DSN_Databricks__3343",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "odbc:DSN=Databricks;UID=token;PWD=__EncPwd1__",
              "username": "${username_DSN_Databricks__3343}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "DSN_Databricks__3343"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`report`.`arr_alteryx_customer_vw`")
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/DSN_Databricks__3343.yml"
          )
        ),
        input_ports = None
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1046 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_1046",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_1046"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1894 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_1894",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_1894")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2490 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2490",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2490")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2609 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2609",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2609"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2718 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2718",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2718"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2834 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2834",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2834")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2840 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2840",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2840")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2960 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2960",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2960")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3078 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3078",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3078")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3090 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3090",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3090")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3233 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3233",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3233"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3256 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3256",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3256")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3276 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3276",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3276")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3280 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3280",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3280")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3342 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3342",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3342")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__crosstab_3266 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__CrossTab_3266",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__CrossTab_3266"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__crosstab_3267 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__CrossTab_3267",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__CrossTab_3267")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_1840 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_1840",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_1840")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2309 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2309",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2309")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2313 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2313",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2313"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2580 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2580",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2580")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2789 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2789",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2789")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2789_reject = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2789_reject",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2789_reject")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2914 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2914",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2914")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2914_reject = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2914_reject",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2914_reject")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3068 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3068",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3068")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3079 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3079",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3079")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3079_reject = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3079_reject",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3079_reject")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3315 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3315",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3315")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_588 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_588",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_588")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1183 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1183",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1183"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1280 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1280",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1280"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1893_0 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1893_0",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1893_0")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1972_0 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1972_0",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1972_0")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2006_0 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2006_0",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2006_0"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2616_to_formula_2612_1 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2616_to_Formula_2612_1",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2616_to_Formula_2612_1"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2787 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2787",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2787")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2788 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2788",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2788")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2790_to_formula_2826_0 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2790_to_Formula_2826_0",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2790_to_Formula_2826_0"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2820_to_formula_2806_0 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2820_to_Formula_2806_0",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2820_to_Formula_2806_0"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2872_to_formula_2871_0 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2872_to_Formula_2871_0",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2872_to_Formula_2871_0"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_3125_0 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_3125_0",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_3125_0")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_708_0 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_708_0",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_708_0")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__fullstack_stati_2305 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__FullStack_Stati_2305",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__FullStack_Stati_2305")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__fullstack_stati_3115 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__FullStack_Stati_3115",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__FullStack_Stati_3115")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_1012_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1012_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1012_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_1012_right = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1012_right",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1012_right"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_1016_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1016_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1016_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_1016_left = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1016_left",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1016_left"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_1947_left = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1947_left",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1947_left"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_2437_left_unionleftouter = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2437_left_UnionLeftOuter",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2437_left_UnionLeftOuter"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_2621_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2621_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2621_inner"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_267_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_267_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_267_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_2782_inner_unionleftouter = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2782_inner_UnionLeftOuter",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2782_inner_UnionLeftOuter"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_2786_left_unionleftouter = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2786_left_UnionLeftOuter",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2786_left_UnionLeftOuter"
        ),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_2793_left_unionleftouter = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2793_left_UnionLeftOuter",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2793_left_UnionLeftOuter"
        ),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_2969_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2969_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2969_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_3005_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_3005_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_3005_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_3009_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_3009_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_3009_inner"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_3076_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_3076_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_3076_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_3162_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_3162_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_3162_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_387_left_unionleftouter = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_387_left_UnionLeftOuter",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_387_left_UnionLeftOuter"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_595_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_595_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_595_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_595_left = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_595_left",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_595_left"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_700_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_700_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_700_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_884_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_884_inner",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_884_inner"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__multifieldformula_1895 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__MultiFieldFormula_1895",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__MultiFieldFormula_1895"
        ),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__multifieldformula_2966 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__MultiFieldFormula_2966",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__MultiFieldFormula_2966"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__recordid_3003 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__RecordID_3003",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__RecordID_3003")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__recordid_833 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__RecordID_833",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__RecordID_833")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__sort_2642 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_2642",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_2642")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__sort_2643 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_2643",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_2643")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__sort_2646 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_2646",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_2646")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__sort_3016 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_3016",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_3016")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__sort_3064 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_3064",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_3064")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__sort_3065 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_3065",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Sort_3065")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_1027 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1027",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1027")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_1067 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1067",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1067"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_121 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_121",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_121")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_1660 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1660",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1660")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2541 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2541",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2541"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2597 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2597",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2597")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2779 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2779",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2779")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2780 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2780",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2780")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2781 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2781",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2781")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2812 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2812",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2812")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2813 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2813",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2813")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2814 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2814",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2814")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2815 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2815",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2815")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2816 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2816",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2816")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2817 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2817",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2817")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2823 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2823",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2823")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2824 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2824",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2824")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2841 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2841",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2841")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2842 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2842",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2842")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2940 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2940",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2940"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_3004 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_3004",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_3004")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_3128 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_3128",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_3128")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_608 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_608",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_608")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_842 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_842",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_842")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__tadpolesqbandsa_2520 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TadpolesQBandSa_2520",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TadpolesQBandSa_2520")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__textinput_1948_cast = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TextInput_1948_cast",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TextInput_1948_cast")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__textinput_596_cast = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TextInput_596_cast",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TextInput_596_cast")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__union_1837 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_1837",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_1837"),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__union_1980 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_1980",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_1980"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__union_2799 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_2799",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_2799")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__union_3183 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_3183",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_3183")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__union_3307 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_3307",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_3307"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__union_677 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_677",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_677"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__unique_1094 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Unique_1094",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Unique_1094")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__unique_123 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Unique_123",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Unique_123"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__table_3118_input_macro_ip = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__table_3118_Input_macro_ip",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__table_3118_Input_macro_ip"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__table_3124_engine_records_macro_ip = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__table_3124_Engine_Records_macro_ip",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__table_3124_Engine_Records_macro_ip"
        ),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    dbfileinput_243_2435 = Process(
        name = "DbFileInput_243_2435",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\GL Detail - Refunds and Reductions.yxdb",
              "username": "${username_DbFileInput_243_2435}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "DbFileInput_243_2435"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\GL Detail - Refunds and Reductions.yxdb"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/DbFileInput_243_2435.yml"
          )
        ),
        input_ports = None
    )
    dbfileinput_308_3089 = Process(
        name = "DbFileInput_308_3089",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Winner Loser not in SFDC.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation(includeSheetNameColumn = False)
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/DbFileInput_308_3089.yml",
            sheetName = "Sheet1",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    dbfileinput_323_3230 = Process(
        name = "DbFileInput_323_3230",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Historical Tadpoles.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation(includeSheetNameColumn = False)
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/DbFileInput_323_3230.yml",
            sheetName = "Sales Details",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    dynamicrename_3273 = Process(
        name = "DynamicRename_3273",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nfrom pyspark.sql.functions import col\n\n# Fixed list of input columns to rename\nrename_cols = [\"2024\", \"2025\", \"2023\", \"2026\", \"Product\", \"Stage\"]\n\n# Collect new names from right input\nnew_names = [row[\"Updated Name\"] for row in in1.select(\"Updated Name\").collect()]\n\n\n# Build rename mapping\nrename_map = dict(zip(rename_cols, new_names))\n\n# Build final select expressions (preserve column order)\noutputCols = [\n    col(c).alias(rename_map[c]) if c in rename_map else col(c)\n    for c in in0.columns\n]\n\nout0 = in0.select(*outputCols)\n"
        ),
        input_ports = 2,
        is_custom_output_schema = True
    )
    dynamicrename_3277 = Process(
        name = "DynamicRename_3277",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nfrom pyspark.sql.functions import col\n\n# Fixed list of input columns to rename\nrename_cols = [\"2024\", \"2025\", \"2023\", \"2026\", \"Product\", \"Stage\"]\n\n# Collect new names from right input\nnew_names = [row[\"Updated Name\"] for row in in1.select(\"Updated Name\").collect()]\n\n\n# Build rename mapping\nrename_map = dict(zip(rename_cols, new_names))\n\n# Build final select expressions (preserve column order)\noutputCols = [\n    col(c).alias(rename_map[c]) if c in rename_map else col(c)\n    for c in in0.columns\n]\n\nout0 = in0.select(*outputCols)\n"
        ),
        input_ports = 2,
        is_custom_output_schema = True
    )
    fullstack_stati_2085 = Process(
        name = "FullStack_Stati_2085",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Outputs\\Static History\\Full Stack - Static History-DB.yxdb",
              "username": "${username_FullStack_Stati_2085}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "FullStack_Stati_2085"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Outputs\\Static History\\Full Stack - Static History-DB.yxdb"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/FullStack_Stati_2085.yml"
          )
        ),
        input_ports = None
    )
    historicaltadpo_3330 = Process(
        name = "HistoricalTadpo_3330",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Static History\\Historical Tadpoles 2.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation(includeSheetNameColumn = False)
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/HistoricalTadpo_3330.yml",
            sheetName = "Sales Details",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    historicalyetto_3323 = Process(
        name = "HistoricalYetTo_3323",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Historical Yet To Renew1.yxdb",
              "username": "${username_HistoricalYetTo_3323}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "HistoricalYetTo_3323"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Historical Yet To Renew1.yxdb"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/HistoricalYetTo_3323.yml"
          )
        ),
        input_ports = None
    )
    macro_3118 = Process(
        name = "Macro_3118",
        properties = PipelineTrigger(
          maxTriggers = 240,
          iterativeInputTable = PipelineTrigger.WarehouseTableName(
            database = {
              "type": "concat_operation",
              "properties": {"elements" : [{"type" : "literal", "properties" : {"value" : "sony"}}]}
            },
            schema = {
              "type": "concat_operation",
              "properties": {"elements" : [{"type" : "literal", "properties" : {"value" : "orch_test"}}]}
            },
            name = {
              "type": "concat_operation",
              "properties": {
                "elements": [{"type" : "literal", "properties" : {"value" : "table_3118_Input_macro_ip"}}]
              }
            }
          ),
          triggerCondition = "Always",
          iteratorMode = True,
          iterativeOutputTable = PipelineTrigger.WarehouseTableName(
            database = {
              "type": "concat_operation",
              "properties": {"elements" : [{"type" : "literal", "properties" : {"value" : "sony"}}]}
            },
            schema = {
              "type": "concat_operation",
              "properties": {"elements" : [{"type" : "literal", "properties" : {"value" : "orch_test"}}]}
            },
            name = {
              "type": "concat_operation",
              "properties": {
                "elements": [{"type" : "literal", "properties" : {"value" : "table_3118_Loop_macro_op"}}]
              }
            }
          ),
          enableMaxTriggers = True,
          pipelineName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118",
          parameters = {}
        ),
        input_ports = None
    )
    macro_3124 = Process(
        name = "Macro_3124",
        properties = PipelineTrigger(
          maxTriggers = 100,
          iterativeInputTable = PipelineTrigger.WarehouseTableName(
            database = {
              "type": "concat_operation",
              "properties": {"elements" : [{"type" : "literal", "properties" : {"value" : "sony"}}]}
            },
            schema = {
              "type": "concat_operation",
              "properties": {"elements" : [{"type" : "literal", "properties" : {"value" : "orch_test"}}]}
            },
            name = {
              "type": "concat_operation",
              "properties": {
                "elements": [{"type" : "literal", "properties" : {"value" : "table_3124_Engine_Records_macro_ip"}}]
              }
            }
          ),
          triggerCondition = "Always",
          iteratorMode = True,
          iterativeOutputTable = PipelineTrigger.WarehouseTableName(
            database = {
              "type": "concat_operation",
              "properties": {"elements" : [{"type" : "literal", "properties" : {"value" : "sony"}}]}
            },
            schema = {
              "type": "concat_operation",
              "properties": {"elements" : [{"type" : "literal", "properties" : {"value" : "orch_test"}}]}
            },
            name = {
              "type": "concat_operation",
              "properties": {
                "elements": [{"type" : "literal", "properties" : {"value" : "table_3124_Loop_macro_op"}}]
              }
            }
          ),
          enableMaxTriggers = True,
          pipelineName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124",
          parameters = {}
        ),
        input_ports = None
    )
    manualadjustmen_564 = Process(
        name = "ManualAdjustmen_564",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Manual Adjustments 08.01.26.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation(includeSheetNameColumn = False)
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            appendRecordIdColumn = True,
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/ManualAdjustmen_564.yml",
            sheetName = "KC Revised",
            recordIdColumnName = "prophecy_recordId_564",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    openrenewals_db_2985 = Process(
        name = "OpenRenewals_DB_2985",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Outputs\\Open Renewals_DB.csv"
          ),
          format = DatabricksVolumeTarget.CsvWriteFormat()
        ),
        output_ports = None
    )
    qualityassist_o_3163 = Process(
        name = "QualityAssist_O_3163",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Quality Assist - OBS Workbook v2 - June.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation(includeSheetNameColumn = False)
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            appendRecordIdColumn = True,
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/QualityAssist_O_3163.yml",
            sheetName = "QB - Post-Close",
            recordIdColumnName = "RecordID",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    quorummagentoda_3179 = Process(
        name = "QuorumMagentoDa_3179",
        properties = DatabricksVolumeTarget(
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed"),
          connector = "databricks_default",
          format = DatabricksVolumeTarget.CsvWriteFormat(),
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Outputs\\Quorum Magento Data_DB.csv"
          )
        ),
        output_ports = None
    )
    savvas_jul26_xl_2449 = Process(
        name = "Savvas_Jul26_xl_2449",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Savvas-Jul 26.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation(includeSheetNameColumn = False)
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/Savvas_Jul26_xl_2449.yml",
            sheetName = "ARRProjectSavvasOrderDetailsvR",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    ts_calculatione_2701 = Process(
        name = "TS_CalculationE_2701",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Outputs\\TS_CalculationEngine_SRD_20250203_Jan25v1_ DB.csv"
          ),
          format = DatabricksVolumeTarget.CsvWriteFormat()
        ),
        output_ports = None
    )
    tad_s2026runrat_2485 = Process(
        name = "Tad_s2026RunRat_2485",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Inputs\\Tad's 2026 Run Rate - July.xlsx",
            fileOperationProperties = DatabricksVolumeSource.SourceFileOperation(includeSheetNameColumn = False)
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            appendRecordIdColumn = True,
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM/Tad_s2026RunRat_2485.yml",
            sheetName = "Sales Details",
            recordIdColumnName = "prophecy_recordId_2485",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    test_1841 = Process(
        name = "Test_1841",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "assert ( in0.count()  == 0 )"
        )
    )
    test_1949 = Process(
        name = "Test_1949",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "assert ( in0.count()  == 0 )"
        )
    )
    test_2308 = Process(
        name = "Test_2308",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "assert ( in0.count()  == 0 )"
        )
    )
    test_2443 = Process(
        name = "Test_2443",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "assert ( in0.count()  == 0 )"
        )
    )
    test_2542 = Process(
        name = "Test_2542",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "assert ( in0.count()  == 0 )"
        )
    )
    test_2614 = Process(
        name = "Test_2614",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "assert ( in0.filter(col(\"Sum_Revenue\") == col(\"Source_Sum_Revenue\")).count() > 0 )"
        )
    )
    test_2941 = Process(
        name = "Test_2941",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "assert ( in0.count()  == 0 )"
        )
    )
    textinput_1202 = Process(
        name = "TextInput_1202",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(
            name = "seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_1202",
            sourceType = "Seed"
          )
        ),
        input_ports = None
    )
    textinput_1948 = Process(
        name = "TextInput_1948",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(
            name = "seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_1948",
            sourceType = "Seed"
          )
        ),
        input_ports = None
    )
    textinput_2348 = Process(
        name = "TextInput_2348",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(
            name = "seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_2348",
            sourceType = "Seed"
          )
        ),
        input_ports = None
    )
    textinput_2620 = Process(
        name = "TextInput_2620",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(
            name = "seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_2620",
            sourceType = "Seed"
          )
        ),
        input_ports = None
    )
    textinput_3324 = Process(
        name = "TextInput_3324",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(
            name = "seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3324",
            sourceType = "Seed"
          )
        ),
        input_ports = None
    )
    textinput_590 = Process(
        name = "TextInput_590",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(
            name = "seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_590",
            sourceType = "Seed"
          )
        ),
        input_ports = None
    )
    textinput_596 = Process(
        name = "TextInput_596",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(
            name = "seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_596",
            sourceType = "Seed"
          )
        ),
        input_ports = None
    )
    yettorenewarr_d_3263 = Process(
        name = "YettoRenewARR_D_3263",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Outputs\\Yet to Renew ARR_DB.csv"
          ),
          format = DatabricksVolumeTarget.CsvWriteFormat()
        ),
        output_ports = None
    )
    table_3118_loop_macro_op = Process(
        name = "table_3118_Loop_macro_op",
        properties = Dataset(
          table = Dataset.DBTSource(name = "table_3118_Loop_macro_op", sourceType = "Table", sourceName = "transpiled_sources"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    table_3124_loop_macro_op = Process(
        name = "table_3124_Loop_macro_op",
        properties = Dataset(
          table = Dataset.DBTSource(name = "table_3124_Loop_macro_op", sourceType = "Table", sourceName = "transpiled_sources"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3280 >> dynamicrename_3277._in(1)
    textinput_1948 >> databricks_calculation_engine_monthly_master_ec2_version_mom__textinput_1948_cast
    (
        dsn_databricks__3336
        >> databricks_calculation_engine_monthly_master_ec2_version_mom__join_387_left_unionleftouter._in(0)
    )
    textinput_596 >> databricks_calculation_engine_monthly_master_ec2_version_mom__textinput_596_cast
    textinput_2348 >> databricks_calculation_engine_monthly_master_ec2_version_mom__union_1837._in(1)
    dbfileinput_308_3089 >> databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3090
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_3004._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_3009_inner._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_3005_inner._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_708_0._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__table_3124_engine_records_macro_ip._in(4),
              databricks_calculation_engine_monthly_master_ec2_version_mom__multifieldformula_1895._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_700_inner._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2006_0._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__union_1980._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom__sort_3016._in(0)]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2940 >> test_2941
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__table_3118_input_macro_ip
        >> batchmacrooutputboundarytruncate_macro_3118
    )
    dsn_databricks__3343 >> databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3342
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3233._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2313._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3256._in(0)]
    )
    historicalyetto_3323 >> databricks_calculation_engine_monthly_master_ec2_version_mom__union_3307._in(0)
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__union_677._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__table_3124_engine_records_macro_ip._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_1027._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2580._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__recordid_3003._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3079._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3079_reject._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__recordid_833._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2597._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_595_left._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_595_inner._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1894._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2006_0._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1972_0._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_1012_inner._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_1067._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1046._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1046._in(2)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2718._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2834._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2840._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__multifieldformula_1895._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_3076_inner._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1893_0._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_387_left_unionleftouter._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_1016_inner._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_1016_left._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2872_to_formula_2871_0._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2609._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2914._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2914_reject._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1046._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_1067._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_3009_inner._in(2)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2788._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2787._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2815._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2823._in(0)]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2541 >> test_2542
    dbfileinput_243_2435 >> databricks_calculation_engine_monthly_master_ec2_version_mom__union_677._in(0)
    manualadjustmen_564 >> databricks_calculation_engine_monthly_master_ec2_version_mom__formula_3125_0
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__crosstab_3266._out(0)
        >> [dynamicrename_3273._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3276._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_1016_left._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2541._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_267_inner._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__union_1837._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__multifieldformula_2966._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__table_3124_engine_records_macro_ip._in(0)]
    )
    dynamicrename_3277 >> databricks_calculation_engine_monthly_master_ec2_version_mom__union_3307._in(2)
    databricks_calculation_engine_monthly_master_ec2_version_mom__union_3307 >> yettorenewarr_d_3263
    tad_s2026runrat_2485 >> databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2490
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__textinput_596_cast._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_1016_inner._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_1016_left._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2597._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_595_left._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__union_677._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2940._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3090._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__unique_123._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__unique_123._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_3162_inner._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1893_0._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1894._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_3076_inner._in(0)]
    )
    qualityassist_o_3163 >> databricks_calculation_engine_monthly_master_ec2_version_mom__union_3183
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_2786_left_unionleftouter._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_2793_left_unionleftouter._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2812._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_2437_left_unionleftouter._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_3009_inner._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1046._in(6),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_1012_inner._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_1012_right._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_3005_inner._in(1)]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_1840 >> test_1841
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2779._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_2793_left_unionleftouter._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2786_left_unionleftouter._in(1)]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__join_1947_left >> test_1949
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__union_2799._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2820_to_formula_2806_0._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2817._in(0)]
    )
    (
        combinedopportu_2436
        >> databricks_calculation_engine_monthly_master_ec2_version_mom__join_2437_left_unionleftouter._in(1)
    )
    dsn_databricks__3338 >> databricks_calculation_engine_monthly_master_ec2_version_mom__union_677._in(1)
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_884_inner._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__table_3124_engine_records_macro_ip._in(6),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_700_inner._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_708_0._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_3128._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1046._in(4),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2969_inner._in(0)]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2960 >> ts_calculatione_2701
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1280._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2780._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2872_to_formula_2871_0._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__recordid_833._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__table_3124_engine_records_macro_ip._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_884_inner._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_842._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__recordid_3003._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_3009_inner._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_3004._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__unique_1094._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__multifieldformula_2966._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__multifieldformula_1895._in(0)]
    )
    dbfileinput_323_3230 >> databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3233._in(0)
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_2782_inner_unionleftouter._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_2786_left_unionleftouter._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2813._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__table_3124_engine_records_macro_ip
        >> batchmacrooutputboundarytruncate_macro_3124
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_608._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2541._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_267_inner._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__union_677._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2940._in(0)]
    )
    textinput_590 >> databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_608
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_1027._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1046._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1046._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_1012_inner._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_1012_right._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2820_to_formula_2806_0._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2609._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__sort_2642._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__sort_2643._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_842._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__table_3124_engine_records_macro_ip._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_884_inner._in(1)]
    )
    dsn_databricks__3340 >> databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_121
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1972_0._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2006_0._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2309._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1183._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1280._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1280._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_1660._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_1012_right._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_1046._in(5),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2969_inner._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2834._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2841._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2621_inner._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2621_inner._in(3)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_1067._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__union_1837._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3315._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2780._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_2793_left_unionleftouter._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2786_left_unionleftouter._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2779._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2790_to_formula_2826_0._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3315._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__crosstab_3267._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__crosstab_3266._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__crosstab_3266._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_2621_inner._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__sort_2646._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2960._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3068._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__crosstab_3267._out(0)
        >> [dynamicrename_3277._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3280._in(0)]
    )
    savvas_jul26_xl_2449 >> databricks_calculation_engine_monthly_master_ec2_version_mom__table_3118_input_macro_ip
    databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3276 >> dynamicrename_3273._in(1)
    databricks_calculation_engine_monthly_master_ec2_version_mom__union_3183 >> quorummagentoda_3179
    dynamicrename_3273 >> databricks_calculation_engine_monthly_master_ec2_version_mom__union_3307._in(1)
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2309 >> test_2308
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__filter_3068._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__sort_3065._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__sort_3064._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2609._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2616_to_formula_2612_1._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2718._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2718._in(1)]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2616_to_formula_2612_1 >> test_2614
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__textinput_1948_cast._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_1947_left._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1183._in(4)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_267_inner._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__filter_1840._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2437_left_unionleftouter._in(0)]
    )
    historicaltadpo_3330 >> databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3233._in(1)
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__unique_123._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__union_1980._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_387_left_unionleftouter._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__union_677._in(4)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3342._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2718._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2718._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1183._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1183._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1183._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2313._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__unique_1094._in(0)]
    )
    textinput_3324 >> databricks_calculation_engine_monthly_master_ec2_version_mom__union_3307._in(3)
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2840._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_2621_inner._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2621_inner._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2842._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2787._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2816._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2824._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2789._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2789_reject._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__union_2799._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__union_1980._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__fullstack_stati_2305._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_1947_left._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1183._in(3)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_3125_0._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__table_3124_engine_records_macro_ip._in(5),
              databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_3128._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__filter_588._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__join_595_left._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_595_inner._in(0)]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2580 >> test_2443
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2790_to_formula_2826_0._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2781._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2782_inner_unionleftouter._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_2782_inner_unionleftouter._in(1)]
    )
    textinput_1202 >> databricks_calculation_engine_monthly_master_ec2_version_mom__formula_1280._in(1)
    textinput_2620 >> databricks_calculation_engine_monthly_master_ec2_version_mom__join_2621_inner._in(4)
    (
        fullstack_stati_2085._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__union_1980._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__fullstack_stati_3115._in(0)]
    )
    dsn_databricks__3331 >> databricks_calculation_engine_monthly_master_ec2_version_mom__filter_588
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_2490._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2313._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__alteryxselect_3078._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__join_2793_left_unionleftouter._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_2814._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__formula_2788._in(0)]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom__multifieldformula_2966 >> openrenewals_db_2985
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__summarize_121._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__unique_123._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom__join_3162_inner._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom__filter_2313._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom__union_1980._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom__tadpolesqbandsa_2520._in(0)]
    )
