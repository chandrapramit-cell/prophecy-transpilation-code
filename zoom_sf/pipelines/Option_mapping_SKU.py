from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Option_mapping_SKU",
    version = 1,
    auto_layout = False,
    params = Parameters(
      VARIABLE1___UPLOAD_FILE_CAY_SAN_PHAM_MOI_NHAT = "''",
      VARIABLE1___UPLOAD_FILE_QUY_UOC_THUOC_TINH_ATTRIBUTE_MOI_NHAT = "''",
      USERNAME_CAYSANPHAMMASTE_233 = "''",
      PASSWORD_CAYSANPHAMMASTE_233 = "''",
      USERNAME_ATTRIBUTECODES__225 = "''",
      PASSWORD_ATTRIBUTECODES__225 = "''",
      JDBCURL_OPTIONMAPPED_SK_244 = "''",
      USERNAME_OPTIONMAPPED_SK_244 = "''",
      PASSWORD_OPTIONMAPPED_SK_244 = "''",
      WORKFLOW_NAME = "'Option_mapping_SKU'",
      QUESTION__FILE_BROWSE_236 = "''",
      QUESTION__FILE_BROWSE_238 = "''"
    )
)

with Pipeline(args) as pipeline:

    with visual_group("SKUOptionmapping"):
        attributecodes__225 = Process(
            name = "Attributecodes__225",
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
                filePath = "C:\\Users\\duonglvm\\OneDrive - TAL Apparel\\2. VNO Projects\\1. Projects\\BI Projects\\4. Redesign\\3. Update Vital tools - after VITAL CAT\\Attribute codes.xlsx"
              ),
              format = SFTPSource.XLSXReadFormat(schema = "external_sources/Option_mapping_SKU/Attributecodes__225.yml")
            ),
            input_ports = None
        )
        caysanphammaste_233 = Process(
            name = "CaySanPhamMaste_233",
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
                filePath = "C:\\Users\\duonglvm\\OneDrive - TAL Apparel\\2. VNO Projects\\1. Projects\\BI Projects\\4. Redesign\\3. Adjust Vital tools - Online incl\\Online Forecast\\Inputs\\Cay San Pham Master 040521.xlsx"
              ),
              format = SFTPSource.XLSXReadFormat(schema = "external_sources/Option_mapping_SKU/CaySanPhamMaste_233.yml")
            ),
            input_ports = None
        )
        dynamicrename_255 = Process(
            name = "DynamicRename_255",
            properties = Script(
              ports = None,
              scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:",
              scriptMethodFooter = "return out0",
              script = "\nfrom pyspark.sql.functions import col\n\n# Fixed list of input columns to rename\nrename_cols = [\"LISTING PRICE\", \"AGE GROUP\", \"DETAILED PRODUCT GROUP\", \"COLOR GROUP\", \"UPPER_MATERIAL\", \"19\", \"23\", \"SIZE RANGE\", \"PRICE GROUP\", \"SPECIALIZED FUNCTION\", \"HEEL HEIGHT\", \"15\", \"COLLECTION\", \"LIFESTYLE GROUP\", \"DESIGN INSPIRATION\", \"22\", \"PRODUCT GROUP\", \"MG GROUP\", \"MCH EXPLANATION\", \"SKU\", \"SIZE\", \"F45\", \"COPYRIGHT GROUP\", \"16\", \"F54\", \"FACTORY\", \"21\", \"DESIGN TEAM\", \"43\", \"MOLD CODE\", \"PRODUCT LINE\", \"STYLE COLOR\", \"LAUNCH SEASON\", \"SIZE GROUP\", \"42\", \"ACTIVITY GROUP\", \"F55\", \"20\", \"MC EXPLANATION\", \"F47\", \"SIMILAR GROUP\", \"18\", \"MCH GROUP\", \"UNIT\", \"BRAND\", \"SOLE_TYPE\", \"NAME EXPLANATION\", \"41\", \"STYLE\", \"GENDER\", \"COLOR\", \"SHOE STYLE\", \"SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE\", \"DEDICATED FUNCTIONS\", \"IMAGE COPYRIGHT\"]\n\n# Collect new names from right input\nnew_names = [row[\"NewName\"] for row in in1.select(\"NewName\").collect()]\n\n\n# Build rename mapping\nrename_map = dict(zip(rename_cols, new_names))\n\n# Build final select expressions (preserve column order)\noutputCols = [\n    col(c).alias(rename_map[c]) if c in rename_map else col(c)\n    for c in in0.columns\n]\n\nout0 = in0.select(*outputCols)\n"
            ),
            input_ports = 2,
            is_custom_output_schema = True
        )
        option_mapping_sku__formula_243_0 = Process(
            name = "Option_mapping_SKU__Formula_243_0",
            properties = ModelTransform(modelName = "Option_mapping_SKU__Formula_243_0"),
            input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8"]
        )
        option_mapping_sku__formula_254_0 = Process(
            name = "Option_mapping_SKU__Formula_254_0",
            properties = ModelTransform(modelName = "Option_mapping_SKU__Formula_254_0")
        )
        option_mapping_sku__textinput_213_cast = Process(
            name = "Option_mapping_SKU__TextInput_213_cast",
            properties = ModelTransform(modelName = "Option_mapping_SKU__TextInput_213_cast")
        )
        optionmapped_sk_244 = Process(
            name = "Optionmapped_SK_244",
            properties = SFTPTarget(
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
              properties = SFTPTarget.SFTPTargetInternal(filePath = "Option mapped-SKU.xlsx"),
              format = SFTPTarget.XLSXWriteFormat()
            )
        )
        textinput_213 = Process(
            name = "TextInput_213",
            properties = Dataset(
              writeOptions = {"writeMode" : "overwrite"},
              table = Dataset.DBTSource(name = "seed_213", sourceType = "Seed")
            ),
            input_ports = None
        )

    (
        attributecodes__225._out(0)
        >> [option_mapping_sku__formula_243_0._in(1), option_mapping_sku__formula_243_0._in(2),
              option_mapping_sku__formula_243_0._in(3), option_mapping_sku__formula_243_0._in(4),
              option_mapping_sku__formula_243_0._in(5), option_mapping_sku__formula_243_0._in(6),
              option_mapping_sku__formula_243_0._in(7), option_mapping_sku__formula_243_0._in(8)]
    )
    option_mapping_sku__formula_243_0 >> optionmapped_sk_244
    option_mapping_sku__formula_254_0 >> dynamicrename_255._in(1)
    textinput_213 >> option_mapping_sku__textinput_213_cast
    caysanphammaste_233._out(0) >> [dynamicrename_255._in(0), option_mapping_sku__formula_254_0._in(0)]
    dynamicrename_255 >> option_mapping_sku__formula_243_0._in(0)
