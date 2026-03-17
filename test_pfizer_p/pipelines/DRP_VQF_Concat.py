from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "DRP_VQF_Concat",
    version = 1,
    auto_layout = False,
    params = Parameters(
      jdbcUrl_vqf_avro_55 = "''",
      username_vqf_avro_55 = "''",
      password_vqf_avro_55 = "''",
      workflow_name = "'DRP_VQF_Concat'"
    )
)

with Pipeline(args) as pipeline:
    drp_vqf_concat__alteryxselect_34 = Process(
        name = "DRP_VQF_Concat__AlteryxSelect_34",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__AlteryxSelect_34"),
        input_ports = ["in_0", "in_1"]
    )
    c1071032_centra_66 = Process(
        name = "C1071032_Centra_66",
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
            filePath = "C:\\cdis\\cdis_phase3_data\\C1071032_Central Labs_Cumulative Electronic Data Vendor Queries Form_21Nov2025.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/DRP_VQF_Concat/C1071032_Centra_66.yml")
        ),
        input_ports = None
    )
    textinput_8 = Process(
        name = "TextInput_8",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_8", sourceType = "Seed")
        ),
        input_ports = None
    )
    dynamicrename_9 = Process(
        name = "DynamicRename_9",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nfrom pyspark.sql.functions import col\n\n# Fixed list of input columns to rename\nrename_cols = [\"Accession #\", \"Response Needed By Date\", \"Missing Samples (Px Codes/LAB ID)\", \"study_id\", \"report_run_date\", \"Requested By\", \"Priority\n(High, Medium, Low)\", \"Lab type\", \"Visit\", \"Inquiry/Request Description\", \"Response or Resolution Details\", \"Inquiry #/Change Request #\", \"Lab Collection Date in Vendor File\", \"Subject ID #\", \"FileName\", \"Request Date \n(dd-Mmm-yyyy)\", \"Response Provided By\", \"Expected/ Response Date \n(dd-Mmm-yyyy)\", \"FINAL Status\", \"Type of Request\n(Inquiry/Change Request)\"]\n\n# Collect new names from right input\nnew_names = [row[\"C1071007\"] for row in in1.select(\"C1071007\").collect()]\n\n\n# Build rename mapping\nrename_map = dict(zip(rename_cols, new_names))\n\n# Build final select expressions (preserve column order)\noutputCols = [\n    col(c).alias(rename_map[c]) if c in rename_map else col(c)\n    for c in in0.columns\n]\n\nout0 = in0.select(*outputCols)\n"
        ),
        input_ports = 2,
        is_custom_output_schema = True
    )
    drp_vqf_concat__alteryxselect_33 = Process(
        name = "DRP_VQF_Concat__AlteryxSelect_33",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__AlteryxSelect_33")
    )
    drp_vqf_concat__formula_26_0 = Process(
        name = "DRP_VQF_Concat__Formula_26_0",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__Formula_26_0")
    )
    drp_vqf_concat__vqf_avro_55 = Process(
        name = "DRP_VQF_Concat__vqf_avro_55",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__vqf_avro_55"),
        input_ports = ["in_0", "in_1"]
    )
    drp_vqf_concat__cleanse_24 = Process(
        name = "DRP_VQF_Concat__Cleanse_24",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__Cleanse_24")
    )
    drp_vqf_concat__cleanse_16 = Process(
        name = "DRP_VQF_Concat__Cleanse_16",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__Cleanse_16")
    )
    drp_vqf_concat__textinput_8_cast = Process(
        name = "DRP_VQF_Concat__TextInput_8_cast",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__TextInput_8_cast")
    )
    drp_vqf_concat__union_69_postrename = Process(
        name = "DRP_VQF_Concat__Union_69_postRename",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__Union_69_postRename"),
        input_ports = ["in_0", "in_1"]
    )
    drp_vqf_concat__formula_27_0 = Process(
        name = "DRP_VQF_Concat__Formula_27_0",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__Formula_27_0")
    )
    drp_vqf_concat__cleanse_21 = Process(
        name = "DRP_VQF_Concat__Cleanse_21",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__Cleanse_21")
    )
    c1071007_lb003c_57 = Process(
        name = "C1071007_LB003C_57",
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
            filePath = "C:\\cdis\\cdis_phase3_data\\C1071007_LB003 Cumulative Electronic Data Vendor Queries Form 04Nov2025.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/DRP_VQF_Concat/C1071007_LB003C_57.yml")
        ),
        input_ports = None
    )
    drp_vqf_concat__join_12_inner = Process(
        name = "DRP_VQF_Concat__Join_12_inner",
        properties = ModelTransform(modelName = "DRP_VQF_Concat__Join_12_inner"),
        input_ports = ["in_0", "in_1"]
    )
    dynamicrename_32 = Process(
        name = "DynamicRename_32",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nfrom pyspark.sql.functions import col\n\n# Fixed list of input columns to rename\nrename_cols = [\"Accession # \n(If Applicable)\", \"Change\nFrom\", \"Expected Vendor Action\", \"Collection Date in eData (Optional)\", \"study_id\", \"report_run_date\", \"Collection Date in CRF\n(If Available)\", \"Requested By\", \"Data Type\", \"Priority\n(High, Medium, Low)\", \"Status 2\", \"Visit\", \"Inquiry/Request Description\", \"Response or Resolution Details\", \"Inquiry #/Change Request #\", \"Subject ID #\", \"DM Comments\", \"Additional Identifier\n(e.g., Lab ID, Test Name)\", \"Change\nTo\", \"Response Needed By\nDate\", \"Site ID\", \"FileName\", \"Request Date \n(dd-Mmm-yyyy)\", \"Response Provided By\", \"Expected/ Response Date \n(dd-Mmm-yyyy)\", \"Type of Request\n(Inquiry/Change Request)\", \"Status\"]\n\n# Collect new names from right input\nnew_names = [row[\"Name\"] for row in in1.select(\"Name\").collect()]\n\n\n# Build rename mapping\nrename_map = dict(zip(rename_cols, new_names))\n\n# Build final select expressions (preserve column order)\noutputCols = [\n    col(c).alias(rename_map[c]) if c in rename_map else col(c)\n    for c in in0.columns\n]\n\nout0 = in0.select(*outputCols)\n"
        ),
        input_ports = 2,
        is_custom_output_schema = True
    )
    c1071032_centra_1 = Process(
        name = "C1071032_Centra_1",
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
            filePath = "C:\\cdis\\DRP_NEW_DATA\\C1071032_Central Labs_Cumulative Electronic Data Vendor Queries Form_06Oct2025.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/DRP_VQF_Concat/C1071032_Centra_1.yml")
        ),
        input_ports = None
    )
    dynamicrename_25 = Process(
        name = "DynamicRename_25",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nfrom pyspark.sql.functions import col\n\n# Fixed list of input columns to rename\nrename_cols = [\"Accession #\", \"Response Needed By Date\", \"Missing Samples (Px Codes/LAB ID)\", \"study_id\", \"report_run_date\", \"Requested By\", \"Priority\n(High, Medium, Low)\", \"Lab type\", \"Visit\", \"Inquiry/Request Description\", \"Response or Resolution Details\", \"Inquiry #/Change Request #\", \"Lab Collection Date in Vendor File\", \"Subject ID #\", \"FileName\", \"Request Date \n(dd-Mmm-yyyy)\", \"Response Provided By\", \"Expected/ Response Date \n(dd-Mmm-yyyy)\", \"FINAL Status\", \"Type of Request\n(Inquiry/Change Request)\"]\n\n# Collect new names from right input\nnew_names = [row[\"Name\"] for row in in1.select(\"Name\").collect()]\n\n\n# Build rename mapping\nrename_map = dict(zip(rename_cols, new_names))\n\n# Build final select expressions (preserve column order)\noutputCols = [\n    col(c).alias(rename_map[c]) if c in rename_map else col(c)\n    for c in in0.columns\n]\n\nout0 = in0.select(*outputCols)\n"
        ),
        input_ports = 2,
        is_custom_output_schema = True
    )
    c1071007_lb003c_2 = Process(
        name = "C1071007_LB003C_2",
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
            filePath = "C:\\cdis\\DRP_NEW_DATA\\C1071007_LB003 Cumulative Electronic Data Vendor Queries Form 11Oct2025.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/DRP_VQF_Concat/C1071007_LB003C_2.yml")
        ),
        input_ports = None
    )
    dynamicrename_28 = Process(
        name = "DynamicRename_28",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nfrom pyspark.sql.functions import col\n\n# Fixed list of input columns to rename\nrename_cols = [\"Type of Request InquiryChange Request\", \"Response Needed By Date\", \"Subject ID\", \"Missing Samples Px CodesLAB ID\", \"Requested By\", \"Lab type\", \"Accession\", \"studyid\", \"Visit\", \"Response or Resolution Details\", \"Lab Collection Date in Vendor File\", \"Inquiry Change Request\", \"FileName\", \"Request Date\", \"Response Provided By\", \"Expected Response Date\", \"reportrundate\", \"FINAL Status\", \"InquiryRequest Description\", \"Priority High Medium Low\"]\n\n# Collect new names from right input\nnew_names = [row[\"C1071007\"] for row in in1.select(\"C1071007\").collect()]\n\n\n# Build rename mapping\nrename_map = dict(zip(rename_cols, new_names))\n\n# Build final select expressions (preserve column order)\noutputCols = [\n    col(c).alias(rename_map[c]) if c in rename_map else col(c)\n    for c in in0.columns\n]\n\nout0 = in0.select(*outputCols)\n"
        ),
        input_ports = 2,
        is_custom_output_schema = True
    )
    textinput_8 >> drp_vqf_concat__textinput_8_cast
    (
        drp_vqf_concat__alteryxselect_33._out(0)
        >> [dynamicrename_9._in(0), dynamicrename_25._in(0), drp_vqf_concat__cleanse_21._in(0)]
    )
    drp_vqf_concat__textinput_8_cast._out(0) >> [dynamicrename_9._in(1), drp_vqf_concat__cleanse_24._in(0)]
    c1071032_centra_66 >> drp_vqf_concat__union_69_postrename._in(0)
    c1071007_lb003c_2 >> drp_vqf_concat__alteryxselect_34._in(0)
    c1071032_centra_1 >> drp_vqf_concat__alteryxselect_33
    drp_vqf_concat__cleanse_16 >> dynamicrename_32._in(1)
    c1071007_lb003c_57 >> drp_vqf_concat__alteryxselect_34._in(1)
    dynamicrename_28 >> drp_vqf_concat__vqf_avro_55._in(1)
    dynamicrename_25 >> dynamicrename_28._in(0)
    (
        drp_vqf_concat__alteryxselect_34._out(0)
        >> [drp_vqf_concat__join_12_inner._in(1), drp_vqf_concat__union_69_postrename._in(1)]
    )
    drp_vqf_concat__union_69_postrename._out(0) >> [dynamicrename_32._in(0), drp_vqf_concat__cleanse_16._in(0)]
    dynamicrename_9 >> drp_vqf_concat__join_12_inner._in(0)
    drp_vqf_concat__cleanse_24._out(0) >> [dynamicrename_28._in(1), drp_vqf_concat__formula_27_0._in(0)]
    dynamicrename_32 >> drp_vqf_concat__vqf_avro_55._in(0)
    drp_vqf_concat__cleanse_21._out(0) >> [dynamicrename_25._in(1), drp_vqf_concat__formula_26_0._in(0)]
