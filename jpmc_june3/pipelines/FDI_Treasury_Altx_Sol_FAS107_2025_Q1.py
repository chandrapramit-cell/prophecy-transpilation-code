from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_ICDW_DEPBAL_CC__269 = "''",
      password_ICDW_DEPBAL_CC__269 = "''",
      jdbcUrl_DbFileOutput_28_281 = "''",
      username_DbFileOutput_28_281 = "''",
      password_DbFileOutput_28_281 = "''",
      jdbcUrl_DbFileOutput_28_289 = "''",
      username_DbFileOutput_28_289 = "''",
      password_DbFileOutput_28_289 = "''",
      jdbcUrl_DbFileOutput_26_266 = "''",
      username_DbFileOutput_26_266 = "''",
      password_DbFileOutput_26_266 = "''",
      jdbcUrl_DbFileOutput_27_277 = "''",
      username_DbFileOutput_27_277 = "''",
      password_DbFileOutput_27_277 = "''",
      workflow_name = "'FDI_Treasury_Altx_Sol_FAS107_2025_Q1'"
    )
)

with Pipeline(args) as pipeline:
    dbfileoutput_26_266 = Process(
        name = "DbFileOutput_26_266",
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
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\NAEAST.ad.jpmorganchase.com\\amercs$\\Group\\wil\\CCBPlanning\\Controller Intelligent Solutions Files\\Treasury\\FAS107 (temp)\\Output\\06-2022 R Code Reconcile.xlsx"
          )
        )
    )
    dbfileoutput_27_277 = Process(
        name = "DbFileOutput_27_277",
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
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\NAEAST.ad.jpmorganchase.com\\amercs$\\Group\\wil\\CCBPlanning\\Controller Intelligent Solutions Files\\Treasury\\FAS107 (temp)\\Output\\06-2022 Hierarchy Input Reconcile.xlsx"
          )
        )
    )
    dbfileoutput_28_281 = Process(
        name = "DbFileOutput_28_281",
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
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\NAEAST.ad.jpmorganchase.com\\amercs$\\Group\\wil\\CCBPlanning\\Controller Intelligent Solutions Files\\Treasury\\FAS107 (temp)\\Output\\06-2022 Maturity_date.xlsx"
          )
        )
    )
    dbfileoutput_28_289 = Process(
        name = "DbFileOutput_28_289",
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
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\NAEAST.ad.jpmorganchase.com\\amercs$\\Group\\wil\\CCBPlanning\\Controller Intelligent Solutions Files\\Treasury\\FAS107 (temp)\\Output\\06-2022 R Code Reconcile.xlsx"
          )
        )
    )
    fdi_treasury_altx_sol_fas107_2025_q1__appendfields_265 = Process(
        name = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__AppendFields_265",
        properties = ModelTransform(modelName = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__AppendFields_265"),
        input_ports = ["in_0", "in_1"]
    )
    fdi_treasury_altx_sol_fas107_2025_q1__appendfields_282 = Process(
        name = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__AppendFields_282",
        properties = ModelTransform(modelName = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__AppendFields_282"),
        input_ports = ["in_0", "in_1"]
    )
    fdi_treasury_altx_sol_fas107_2025_q1__appendfields_283 = Process(
        name = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__AppendFields_283",
        properties = ModelTransform(modelName = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__AppendFields_283"),
        input_ports = ["in_0", "in_1"]
    )
    fdi_treasury_altx_sol_fas107_2025_q1__appendfields_290 = Process(
        name = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__AppendFields_290",
        properties = ModelTransform(modelName = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__AppendFields_290"),
        input_ports = ["in_0", "in_1"]
    )
    fdi_treasury_altx_sol_fas107_2025_q1__filter_251 = Process(
        name = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Filter_251",
        properties = ModelTransform(modelName = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Filter_251"),
        input_ports = ["in_0", "in_1"]
    )
    fdi_treasury_altx_sol_fas107_2025_q1__formula_220_to_formula_286_2 = Process(
        name = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_220_to_Formula_286_2",
        properties = ModelTransform(modelName = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_220_to_Formula_286_2")
    )
    fdi_treasury_altx_sol_fas107_2025_q1__formula_233_0 = Process(
        name = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_233_0",
        properties = ModelTransform(modelName = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_233_0")
    )
    fdi_treasury_altx_sol_fas107_2025_q1__formula_263_to_formula_259_3 = Process(
        name = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_263_to_Formula_259_3",
        properties = ModelTransform(modelName = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_263_to_Formula_259_3")
    )
    fdi_treasury_altx_sol_fas107_2025_q1__formula_273_1 = Process(
        name = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_273_1",
        properties = ModelTransform(modelName = "FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_273_1"),
        input_ports = ["in_0", "in_1"]
    )
    icdw_depbal_cc__269 = Process(
        name = "ICDW_DEPBAL_CC__269",
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
            filePath = "\\\\naeast.ad.jpmorganchase.com\\amercs$\\group\\wil\\CCBPlanning\\Controller Intelligent Solutions Files\\Treasury\\FAS107 (temp)\\ICDW_DEPBAL_CC_WAMU.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/FDI_Treasury_Altx_Sol_FAS107_2025_Q1/ICDW_DEPBAL_CC__269.yml"
          )
        ),
        input_ports = None
    )
    textinput_168 = Process(
        name = "TextInput_168",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_FDI_Treasury_Altx_Sol_FAS107_2025_Q1_168", sourceType = "Seed")
        ),
        input_ports = None
    )
    fdi_treasury_altx_sol_fas107_2025_q1__appendfields_282 >> dbfileoutput_28_281
    fdi_treasury_altx_sol_fas107_2025_q1__appendfields_290 >> dbfileoutput_28_289
    (
        fdi_treasury_altx_sol_fas107_2025_q1__formula_220_to_formula_286_2._out(0)
        >> [fdi_treasury_altx_sol_fas107_2025_q1__appendfields_283._in(0),
              fdi_treasury_altx_sol_fas107_2025_q1__appendfields_282._in(0),
              fdi_treasury_altx_sol_fas107_2025_q1__appendfields_290._in(0),
              fdi_treasury_altx_sol_fas107_2025_q1__appendfields_265._in(0),
              fdi_treasury_altx_sol_fas107_2025_q1__filter_251._in(0)]
    )
    (
        fdi_treasury_altx_sol_fas107_2025_q1__formula_263_to_formula_259_3._out(0)
        >> [fdi_treasury_altx_sol_fas107_2025_q1__appendfields_290._in(1),
              fdi_treasury_altx_sol_fas107_2025_q1__appendfields_265._in(1)]
    )
    (
        fdi_treasury_altx_sol_fas107_2025_q1__filter_251._out(0)
        >> [fdi_treasury_altx_sol_fas107_2025_q1__formula_273_1._in(1),
              fdi_treasury_altx_sol_fas107_2025_q1__formula_263_to_formula_259_3._in(0)]
    )
    fdi_treasury_altx_sol_fas107_2025_q1__appendfields_283 >> dbfileoutput_27_277
    (
        fdi_treasury_altx_sol_fas107_2025_q1__formula_233_0._out(0)
        >> [fdi_treasury_altx_sol_fas107_2025_q1__filter_251._in(1),
              fdi_treasury_altx_sol_fas107_2025_q1__formula_220_to_formula_286_2._in(0)]
    )
    textinput_168 >> fdi_treasury_altx_sol_fas107_2025_q1__formula_233_0
    fdi_treasury_altx_sol_fas107_2025_q1__appendfields_265 >> dbfileoutput_26_266
    icdw_depbal_cc__269 >> fdi_treasury_altx_sol_fas107_2025_q1__formula_273_1._in(0)
    (
        fdi_treasury_altx_sol_fas107_2025_q1__formula_273_1._out(0)
        >> [fdi_treasury_altx_sol_fas107_2025_q1__appendfields_283._in(1),
              fdi_treasury_altx_sol_fas107_2025_q1__appendfields_282._in(1)]
    )
