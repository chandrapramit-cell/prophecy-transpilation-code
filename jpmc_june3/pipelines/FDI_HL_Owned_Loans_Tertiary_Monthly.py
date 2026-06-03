from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "FDI_HL_Owned_Loans_Tertiary_Monthly",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_Source__User_Db_42 = "''",
      password_Source__User_Db_42 = "''",
      username_Source__User_Db_45 = "''",
      password_Source__User_Db_45 = "''",
      workflow_name = "'FDI_HL_Owned_Loans_Tertiary_Monthly'",
      User__DbConnectionFilePath = "'File:D:\\ccbnas\\config\\db_connections\\hl-alteryx-dbx-connection.indbc'"
    )
)

with Pipeline(args) as pipeline:
    fdi_hl_owned_loans_tertiary_monthly__formula_41_0 = Process(
        name = "FDI_HL_Owned_Loans_Tertiary_Monthly__Formula_41_0",
        properties = ModelTransform(modelName = "FDI_HL_Owned_Loans_Tertiary_Monthly__Formula_41_0"),
        input_ports = ["in_0", "in_1"]
    )
    fdi_hl_owned_loans_tertiary_monthly__macro_48 = Process(
        name = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_48",
        properties = ModelTransform(modelName = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_48")
    )
    fdi_hl_owned_loans_tertiary_monthly__macro_50 = Process(
        name = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_50",
        properties = ModelTransform(modelName = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_50")
    )
    source__user_db_42 = Process(
        name = "Source__User_Db_42",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "jdbc://<PLEASE_EDIT>(%User.DbConnectionFilePath%)",
              "username": "${username_Source__User_Db_42}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Source__User_Db_42"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "\nSelect\n  ldgr_acru_sts_in,\n  accrual_type,\n  acct_gen1_nm,\n  acct_gen1_alias_nm,\n  acct_gen10_nm,\n  acct_gen10_alias_nm,\n  acct_gen17_nm,\n  acct_gen17_alias_nm,\n  acct_gen2_nm,\n  acct_gen2_alias_nm,\n  acct_gen3_nm,\n  acct_gen3_alias_nm,\n  acct_gen4_nm,\n  acct_gen4_alias_nm,\n  acct_gen5_nm,\n  acct_gen5_alias_nm,\n  acct_gen6_nm,\n  acct_gen6_alias_nm,\n  acct_gen7_nm,\n  acct_gen7_alias_nm,\n  acct_gen8_nm,\n  acct_gen8_alias_nm,\n  acct_gen9_nm,\n  acct_gen9_alias_nm,\n  arm_int_rt_max_life_ceil_rt,\n  arm_int_rt_max_life_flr_rt,\n  bal_am,\n  bal_sub_type,\n  bal_type,\n  rcc_call_cd,\n  call_cd,\n  cat_cd,\n  chrgf_type,\n  co_nb,\n  construction_in,\n  cnsmr_loan_in,\n  ctnt_nm,\n  business_status,\n  cc_nb,\n  org_alias_nm,\n  cntry_ad_cd,\n  cntry_nm,\n  credit_impaired,\n  ots_delq_gp_cd,\n  acct_trtm_cd,\n  FICO_RANGE,\n  frst_prin_cc_nb,\n  frcls_in,\n  frgn_ad_in,\n  GEO_REGION,\n  gl_acct_nb,\n  govt_ins_in,\n  hfs_in,\n  heritage,\n  hfs_hfi,\n  int_only_in,\n  int_only_expr_dt,\n  note_int_rt,\n  ivst_id,\n  ivst_type_cd,\n  ivst_type_sbsd_cd,\n  jmbo_cfrm_id,\n  bnk_nonbnk_flag,\n  lien_prrty_cd,\n  lien_pstn_cd,\n  line_item,\n  loan_type,\n  loan_type_desc,\n  loan_nb,\n  lob,\n  LTV_RANGE,\n  arm_mrgn_rt,\n  loan_mtr_dt,\n  loan_mtr_sts,\n  org_cd,\n  neg_amz_in,\n  nxt_pymt_due_dt,\n  nxt_rt_chg_dt,\n  nre_in,\n  opt_arm_in,\n  gen2_nm,\n  gen2_alias_nm,\n  gen3_nm,\n  gen3_alias_nm,\n  gen4_nm,\n  gen4_alias_nm,\n  gen9_nm,\n  orgn_dt,\n  otst_in,\n  own_srcvd_frst_cd,\n  payee_id,\n  product,\n  reo_in,\n  rmn_term,\n  lob_desc,\n  orgn_sys_cd,\n  std_adj_in,\n  prop_state_cd,\n  stop_advn_in,\n  sub_lob_desc,\n  sys_acru_sts_in,\n  tdr_in,\n  loan_term,\n  var_rt_in,\n  ci_nci_cd,\n  pldg_loan_to_cd,\n  max_neg_amz_rmn_am,\n  nxt_rprc_sts_rpt_cd,\n  sub_lob_id,\n  tot_neg_amz_rmn_incur_am,\n  bus_mo,\n  bus_dt,\n  user_03_position_field_6c,\n  loan_loss_shar_agrm_in,\n  fdic_loan_nb,\n  loan_loss_shar_agrm_cd\n\nfrom\n  90278_ctg_prod.fdi_refined_mb_srvc_datamart_schema.mb_own_loan_rpt\nwhere\n  bus_mo >= from_unixtime(unix_timestamp(add_months(now(), -3)), 'yyyyMM')"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/FDI_HL_Owned_Loans_Tertiary_Monthly/Source__User_Db_42.yml"
          )
        ),
        input_ports = None
    )
    source__user_db_45 = Process(
        name = "Source__User_Db_45",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "jdbc://<PLEASE_EDIT>(%User.DbConnectionFilePath%)",
              "username": "${username_Source__User_Db_45}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Source__User_Db_45"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "\nSelect\n  ldgr_acru_sts_in,\n  accrual_type,\n  acct_gen1_nm,\n  acct_gen1_alias_nm,\n  acct_gen10_nm,\n  acct_gen10_alias_nm,\n  acct_gen17_nm,\n  acct_gen17_alias_nm,\n  acct_gen2_nm,\n  acct_gen2_alias_nm,\n  acct_gen3_nm,\n  acct_gen3_alias_nm,\n  acct_gen4_nm,\n  acct_gen4_alias_nm,\n  acct_gen5_nm,\n  acct_gen5_alias_nm,\n  acct_gen6_nm,\n  acct_gen6_alias_nm,\n  acct_gen7_nm,\n  acct_gen7_alias_nm,\n  acct_gen8_nm,\n  acct_gen8_alias_nm,\n  acct_gen9_nm,\n  acct_gen9_alias_nm,\n  arm_int_rt_max_life_ceil_rt,\n  arm_int_rt_max_life_flr_rt,\n  bal_am,\n  bal_sub_type,\n  bal_type,\n  rcc_call_cd,\n  call_cd,\n  cat_cd,\n  chrgf_type,\n  co_nb,\n  construction_in,\n  cnsmr_loan_in,\n  ctnt_nm,\n  business_status,\n  cc_nb,\n  org_alias_nm,\n  cntry_ad_cd,\n  cntry_nm,\n  credit_impaired,\n  ots_delq_gp_cd,\n  acct_trtm_cd,\n  FICO_RANGE,\n  frst_prin_cc_nb,\n  frcls_in,\n  frgn_ad_in,\n  GEO_REGION,\n  gl_acct_nb,\n  govt_ins_in,\n  hfs_in,\n  heritage,\n  hfs_hfi,\n  int_only_in,\n  int_only_expr_dt,\n  note_int_rt,\n  ivst_id,\n  ivst_type_cd,\n  ivst_type_sbsd_cd,\n  jmbo_cfrm_id,\n  bnk_nonbnk_flag,\n  lien_prrty_cd,\n  lien_pstn_cd,\n  line_item,\n  loan_type,\n  loan_type_desc,\n  loan_nb,\n  lob,\n  LTV_RANGE,\n  arm_mrgn_rt,\n  loan_mtr_dt,\n  loan_mtr_sts,\n  org_cd,\n  neg_amz_in,\n  nxt_pymt_due_dt,\n  nxt_rt_chg_dt,\n  nre_in,\n  opt_arm_in,\n  gen2_nm,\n  gen2_alias_nm,\n  gen3_nm,\n  gen3_alias_nm,\n  gen4_nm,\n  gen4_alias_nm,\n  gen9_nm,\n  orgn_dt,\n  otst_in,\n  own_srcvd_frst_cd,\n  payee_id,\n  product,\n  reo_in,\n  rmn_term,\n  lob_desc,\n  orgn_sys_cd,\n  std_adj_in,\n  prop_state_cd,\n  stop_advn_in,\n  sub_lob_desc,\n  sys_acru_sts_in,\n  tdr_in,\n  loan_term,\n  var_rt_in,\n  ci_nci_cd,\n  pldg_loan_to_cd,\n  max_neg_amz_rmn_am,\n  nxt_rprc_sts_rpt_cd,\n  sub_lob_id,\n  tot_neg_amz_rmn_incur_am,\n  bus_mo,\n  bus_dt,\n  user_03_position_field_6c,\n  loan_loss_shar_agrm_in,\n  fdic_loan_nb,\n  loan_loss_shar_agrm_cd\nfrom\n  90278_ctg_prod.fdi_refined_mb_srvc_datamart_schema.mb_own_loan_rpt\nwhere\n  bus_mo between from_unixtime(unix_timestamp(add_months(now(), -6)), 'yyyyMM')\n  and from_unixtime(unix_timestamp(add_months(now(), -4)), 'yyyyMM')\n"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/FDI_HL_Owned_Loans_Tertiary_Monthly/Source__User_Db_45.yml"
          )
        ),
        input_ports = None
    )
    source__user_db_42 >> fdi_hl_owned_loans_tertiary_monthly__formula_41_0._in(0)
    source__user_db_45 >> fdi_hl_owned_loans_tertiary_monthly__formula_41_0._in(1)
    (
        fdi_hl_owned_loans_tertiary_monthly__formula_41_0._out(0)
        >> [fdi_hl_owned_loans_tertiary_monthly__macro_48._in(0), fdi_hl_owned_loans_tertiary_monthly__macro_50._in(0)]
    )
