from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "DRP_LAB_RECON_Concat",
    version = 1,
    auto_layout = False,
    params = Parameters(
      jdbcUrl_lab_reconciliat_19 = "'C:\\\\cdis\\\\cdis_phase3_data\\\\lab_reconciliation.avro'",
      username_lab_reconciliat_19 = "''",
      password_lab_reconciliat_19 = "''",
      workflow_name = "'DRP_LAB_RECON_Concat'"
    )
)

with Pipeline(args) as pipeline:
    drp_lab_recon_concat__lab_reconciliat_19 = Process(
        name = "DRP_LAB_RECON_Concat__lab_reconciliat_19",
        properties = ModelTransform(modelName = "DRP_LAB_RECON_Concat__lab_reconciliat_19"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    c1071007_lb003__35 = Process(
        name = "C1071007_LB003__35",
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
            filePath = "C:\\cdis\\cdis_phase3_data\\data compare\\New folder\\C1071007_LB003_DR_C003_LAB_RECONCILIATION_20251103140247696914000.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/DRP_LAB_RECON_Concat/C1071007_LB003__35.yml")
        ),
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
    drp_lab_recon_concat__textinput_5_cast = Process(
        name = "DRP_LAB_RECON_Concat__TextInput_5_cast",
        properties = ModelTransform(modelName = "DRP_LAB_RECON_Concat__TextInput_5_cast")
    )
    c1071007_lb003__1 = Process(
        name = "C1071007_LB003__1",
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
            filePath = "C:\\cdis\\DRP_NEW_DATA\\C1071007_LB003_DR_C003_LAB_RECONCILIATION_20251023140206355409000.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/DRP_LAB_RECON_Concat/C1071007_LB003__1.yml")
        ),
        input_ports = None
    )
    dynamicrename_7 = Process(
        name = "DynamicRename_7",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nfrom pyspark.sql.functions import col\n\n# Fixed list of input columns to rename\nrename_cols = [\"Aging\", \"Lab Date of Collection\", \"Study ID\", \"Subject Number\", \"Study Center Name\", \"Discrepancy category\", \"Date of Visit (InForm)\", \"Lab Type\", \"Related Samples\", \"CUS_VISITMAP\", \"FileName\", \"Lab Time of Collection\", \"Enrollment Group\", \"Visit Name (Central Labs)\", \"RecordID\", \"Visit Name (InForm)\", \"Accession Number\", \"Site Country\", \"Main IC Date (InForm)\"]\n\n# Collect new names from right input\nnew_names = [row[\"C1071007\"] for row in in1.select(\"C1071007\").collect()]\n\n\n# Build rename mapping\nrename_map = dict(zip(rename_cols, new_names))\n\n# Build final select expressions (preserve column order)\noutputCols = [\n    col(c).alias(rename_map[c]) if c in rename_map else col(c)\n    for c in in0.columns\n]\n\nout0 = in0.select(*outputCols)\n"
        ),
        input_ports = 2,
        is_custom_output_schema = True
    )
    drp_lab_recon_concat__union_41 = Process(
        name = "DRP_LAB_RECON_Concat__Union_41",
        properties = ModelTransform(modelName = "DRP_LAB_RECON_Concat__Union_41"),
        input_ports = ["in_0", "in_1"]
    )
    c1071032_lb003__3 = Process(
        name = "C1071032_LB003__3",
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
            filePath = "C:\\cdis\\DRP_NEW_DATA\\C1071032_LB003_DR_S001_CENTRAL_LAB_RECONCILIATION_REPORT_20251023130248675967000.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/DRP_LAB_RECON_Concat/C1071032_LB003__3.yml")
        ),
        input_ports = None
    )
    c1071032_lb003__52 = Process(
        name = "C1071032_LB003__52",
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
            filePath = "C:\\cdis\\cdis_phase3_data\\data compare\\C1071032_LB003_DR_S001_CENTRAL_LAB_RECONCILIATION_REPORT_20251121130345613597000.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/DRP_LAB_RECON_Concat/C1071032_LB003__52.yml")
        ),
        input_ports = None
    )
    dynamicrename_7 >> drp_lab_recon_concat__lab_reconciliat_19._in(1)
    drp_lab_recon_concat__textinput_5_cast >> dynamicrename_7._in(1)
    c1071007_lb003__1 >> drp_lab_recon_concat__lab_reconciliat_19._in(0)
    drp_lab_recon_concat__union_41 >> dynamicrename_7._in(0)
    c1071032_lb003__52 >> drp_lab_recon_concat__union_41._in(1)
    c1071032_lb003__3 >> drp_lab_recon_concat__union_41._in(0)
    textinput_5 >> drp_lab_recon_concat__textinput_5_cast
    c1071007_lb003__35 >> drp_lab_recon_concat__lab_reconciliat_19._in(2)
