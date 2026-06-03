from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_p_202504_xlsx_Q_35 = "''",
      password_p_202504_xlsx_Q_35 = "''",
      username_state_202504_xl_34 = "''",
      password_state_202504_xl_34 = "''",
      username_tax_bill_202504_33 = "''",
      password_tax_bill_202504_33 = "''",
      jdbcUrl_output_xlsx_Que_51 = "''",
      username_output_xlsx_Que_51 = "''",
      password_output_xlsx_Que_51 = "''",
      jdbcUrl_output_xlsx_Que_50 = "''",
      username_output_xlsx_Que_50 = "''",
      password_output_xlsx_Que_50 = "''",
      workflow_name = "'Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final'"
    )
)

with Pipeline(args) as pipeline:
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__alteryxselect_36 = Process(
        name = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__AlteryxSelect_36",
        properties = ModelTransform(modelName = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__AlteryxSelect_36")
    )
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__alteryxselect_37 = Process(
        name = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__AlteryxSelect_37",
        properties = ModelTransform(modelName = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__AlteryxSelect_37")
    )
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__formula_56_0 = Process(
        name = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Formula_56_0",
        properties = ModelTransform(modelName = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Formula_56_0")
    )
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__formula_57_0 = Process(
        name = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Formula_57_0",
        properties = ModelTransform(modelName = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Formula_57_0")
    )
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__summarize_52 = Process(
        name = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Summarize_52",
        properties = ModelTransform(modelName = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Summarize_52")
    )
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__summarize_53 = Process(
        name = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Summarize_53",
        properties = ModelTransform(modelName = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Summarize_53")
    )
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__union_46 = Process(
        name = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Union_46",
        properties = ModelTransform(modelName = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Union_46"),
        input_ports = ["in_0", "in_1"]
    )
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__union_47 = Process(
        name = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Union_47",
        properties = ModelTransform(modelName = "Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Union_47"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    output_xlsx_que_50 = Process(
        name = "output_xlsx_Que_50",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
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
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(filePath = ".\\output.xlsx")
        )
    )
    output_xlsx_que_51 = Process(
        name = "output_xlsx_Que_51",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
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
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(filePath = ".\\output.xlsx")
        )
    )
    p_202504_xlsx_q_35 = Process(
        name = "p_202504_xlsx_Q_35",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "H:\\SAS Process\\SAS Conversion Project\\BD4-Tax\\p_202504.xlsx"),
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final/p_202504_xlsx_Q_35.yml"
          )
        ),
        input_ports = None
    )
    state_202504_xl_34 = Process(
        name = "state_202504_xl_34",
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
            filePath = "H:\\SAS Process\\SAS Conversion Project\\BD4-Tax\\state_202504.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final/state_202504_xl_34.yml"
          )
        ),
        input_ports = None
    )
    tax_bill_202504_33 = Process(
        name = "tax_bill_202504_33",
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
            filePath = "H:\\SAS Process\\SAS Conversion Project\\BD4-Tax\\tax_bill_202504.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final/tax_bill_202504_33.yml"
          )
        ),
        input_ports = None
    )
    state_202504_xl_34 >> op_lease_tax_billing_and_tax_income_prophecy_usecase_final__alteryxselect_36
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__formula_56_0 >> output_xlsx_que_50
    (
        op_lease_tax_billing_and_tax_income_prophecy_usecase_final__alteryxselect_37._out(0)
        >> [op_lease_tax_billing_and_tax_income_prophecy_usecase_final__union_47._in(1),
              op_lease_tax_billing_and_tax_income_prophecy_usecase_final__union_46._in(0)]
    )
    (
        op_lease_tax_billing_and_tax_income_prophecy_usecase_final__union_47._out(0)
        >> [op_lease_tax_billing_and_tax_income_prophecy_usecase_final__summarize_53._in(0),
              op_lease_tax_billing_and_tax_income_prophecy_usecase_final__formula_57_0._in(0)]
    )
    (
        op_lease_tax_billing_and_tax_income_prophecy_usecase_final__alteryxselect_36._out(0)
        >> [op_lease_tax_billing_and_tax_income_prophecy_usecase_final__union_47._in(2),
              op_lease_tax_billing_and_tax_income_prophecy_usecase_final__union_46._in(1)]
    )
    (
        op_lease_tax_billing_and_tax_income_prophecy_usecase_final__union_46._out(0)
        >> [op_lease_tax_billing_and_tax_income_prophecy_usecase_final__formula_56_0._in(0),
              op_lease_tax_billing_and_tax_income_prophecy_usecase_final__summarize_52._in(0)]
    )
    tax_bill_202504_33 >> op_lease_tax_billing_and_tax_income_prophecy_usecase_final__alteryxselect_37
    op_lease_tax_billing_and_tax_income_prophecy_usecase_final__formula_57_0 >> output_xlsx_que_51
    p_202504_xlsx_q_35 >> op_lease_tax_billing_and_tax_income_prophecy_usecase_final__union_47._in(0)
