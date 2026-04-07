from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Alpha_workflow_update1",
    version = 1,
    auto_layout = False,
    params = Parameters(
      USERNAME_ALPHA_1__1__XLS_1 = "''",
      PASSWORD_ALPHA_1__1__XLS_1 = "''",
      USERNAME_GEN_00_DATAKEY__28 = "''",
      PASSWORD_GEN_00_DATAKEY__28 = "''",
      USERNAME_GEN_DEPT_ID_ALL_199 = "''",
      PASSWORD_GEN_DEPT_ID_ALL_199 = "''",
      USERNAME_GEN_UNION_JOBS__485 = "''",
      PASSWORD_GEN_UNION_JOBS__485 = "''",
      USERNAME_ACCOUNTSDATA_XL_311 = "''",
      PASSWORD_ACCOUNTSDATA_XL_311 = "''",
      USERNAME_WORK_SCHEDULEVA_335 = "''",
      PASSWORD_WORK_SCHEDULEVA_335 = "''",
      WORKFLOW_NAME = "'Alpha_workflow_update1'"
    )
)

with Pipeline(args) as pipeline:

    with visual_group("AccountCode"):
        pass

    with visual_group("BAcompanymismatch"):
        pass

    with visual_group("BUAcctCodeMismatch"):
        pass

    with visual_group("BUAcctCodeMismatch"):
        pass

    with visual_group("BUBAMismatch"):
        pass

    with visual_group("DeptCodefixedononboarding"):
        pass

    with visual_group("RAProblems"):
        pass

    with visual_group("UnionJobError"):
        pass

    with visual_group("WorkSchedule"):
        pass

    accountsdata_xl_311 = Process(
        name = "Accountsdata_xl_311",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "_externals\\1\\Accountsdata.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Alpha_workflow_update1/Accountsdata_xl_311.yml")
        ),
        input_ports = None
    )
    alpha_workflow_update1__alteryxselect_336 = Process(
        name = "Alpha_workflow_update1__AlteryxSelect_336",
        properties = ModelTransform(modelName = "Alpha_workflow_update1__AlteryxSelect_336")
    )
    alpha_workflow_update1__alteryxselect_515 = Process(
        name = "Alpha_workflow_update1__AlteryxSelect_515",
        properties = ModelTransform(modelName = "Alpha_workflow_update1__AlteryxSelect_515")
    )
    alpha_workflow_update1__alteryxselect_582 = Process(
        name = "Alpha_workflow_update1__AlteryxSelect_582",
        properties = ModelTransform(modelName = "Alpha_workflow_update1__AlteryxSelect_582"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17", "in_18"]
    )
    alpha_workflow_update1__filter_345_reject = Process(
        name = "Alpha_workflow_update1__Filter_345_reject",
        properties = ModelTransform(modelName = "Alpha_workflow_update1__Filter_345_reject")
    )
    alpha_workflow_update1__filter_347_reject = Process(
        name = "Alpha_workflow_update1__Filter_347_reject",
        properties = ModelTransform(modelName = "Alpha_workflow_update1__Filter_347_reject")
    )
    alpha_workflow_update1__join_346_inner = Process(
        name = "Alpha_workflow_update1__Join_346_inner",
        properties = ModelTransform(modelName = "Alpha_workflow_update1__Join_346_inner"),
        input_ports = ["in_0", "in_1"]
    )
    gen_00_datakey__28 = Process(
        name = "Gen_00_datakey__28",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "_externals\\1\\Gen_00_data key.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Alpha_workflow_update1/Gen_00_datakey__28.yml")
        ),
        input_ports = None
    )
    gen_union_jobs__485 = Process(
        name = "Gen_Union_jobs__485",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "_externals\\1\\Gen_Union_jobs.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Alpha_workflow_update1/Gen_Union_jobs__485.yml")
        ),
        input_ports = None
    )
    gen_dept_id_all_199 = Process(
        name = "Gen_dept_id_all_199",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "_externals\\1\\Gen_dept_id_all.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Alpha_workflow_update1/Gen_dept_id_all_199.yml")
        ),
        input_ports = None
    )
    work_scheduleva_335 = Process(
        name = "Work_ScheduleVa_335",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "_externals\\1\\Work_Schedule Validation table.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Alpha_workflow_update1/Work_ScheduleVa_335.yml")
        ),
        input_ports = None
    )
    alpha_1__1__xls_1 = Process(
        name = "alpha_1__1__xls_1",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "_externals\\2\\alpha (1) (1).xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Alpha_workflow_update1/alpha_1__1__xls_1.yml")
        ),
        input_ports = None
    )
    (
        alpha_workflow_update1__filter_345_reject._out(0)
        >> [alpha_workflow_update1__alteryxselect_582._in(5), alpha_workflow_update1__join_346_inner._in(0)]
    )
    gen_dept_id_all_199 >> alpha_workflow_update1__alteryxselect_582._in(0)
    (
        accountsdata_xl_311._out(0)
        >> [alpha_workflow_update1__alteryxselect_582._in(1), alpha_workflow_update1__alteryxselect_582._in(14),
              alpha_workflow_update1__alteryxselect_582._in(15)]
    )
    (
        alpha_workflow_update1__alteryxselect_515._out(0)
        >> [alpha_workflow_update1__alteryxselect_582._in(2), alpha_workflow_update1__alteryxselect_582._in(8),
              alpha_workflow_update1__alteryxselect_582._in(13),
              alpha_workflow_update1__alteryxselect_582._in(17),
              alpha_workflow_update1__alteryxselect_582._in(18),
              alpha_workflow_update1__alteryxselect_336._in(0)]
    )
    (
        alpha_workflow_update1__filter_347_reject._out(0)
        >> [alpha_workflow_update1__alteryxselect_582._in(6), alpha_workflow_update1__join_346_inner._in(1)]
    )
    (
        alpha_workflow_update1__alteryxselect_336._out(0)
        >> [alpha_workflow_update1__alteryxselect_582._in(4), alpha_workflow_update1__filter_347_reject._in(0)]
    )
    (
        work_scheduleva_335._out(0)
        >> [alpha_workflow_update1__alteryxselect_582._in(3), alpha_workflow_update1__filter_345_reject._in(0)]
    )
    alpha_1__1__xls_1 >> alpha_workflow_update1__alteryxselect_515
    (
        gen_00_datakey__28._out(0)
        >> [alpha_workflow_update1__alteryxselect_582._in(9), alpha_workflow_update1__alteryxselect_582._in(10),
              alpha_workflow_update1__alteryxselect_582._in(11),
              alpha_workflow_update1__alteryxselect_582._in(12),
              alpha_workflow_update1__alteryxselect_582._in(16)]
    )
    gen_union_jobs__485 >> alpha_workflow_update1__alteryxselect_582._in(7)
