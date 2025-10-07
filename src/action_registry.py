import os
import logging
from dateutil.relativedelta import relativedelta
import pandas as pd
 
# --- Configuration & Setup ---
# 임시로 이 파일에 설정
CONFIG = {
    "project_root": os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../..")),
    "data_dir": "src/modules/agents/analysis/utils",
    "log_level": logging.INFO,
}
from src.modules.agents.analysis.orm.postgres import RDB
 
# Setup basic logging
logging.basicConfig(
    level=CONFIG["log_level"],
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
)
 
# --- Helpers ---
def safe_read_csv(file_path, dtype=None):
    """Reads a CSV file safely, returning an empty DataFrame on failure."""
    try:
        return pd.read_csv(file_path, dtype=dtype)
    except FileNotFoundError:
        logging.warning(f"File not found: {file_path}")
    except pd.errors.EmptyDataError:
        logging.warning(f"Empty CSV file: {file_path}")
    except Exception as e:
        logging.error(f"Failed to read CSV {file_path}: {e}", exc_info=True)
    return pd.DataFrame()
 
def safe_db_call(func, *args, **kwargs):
    """Wraps a database call for safe execution, returning None on failure."""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logging.error(f"DB call {func.__name__} failed: {e}", exc_info=True)
        return None
 
def safe_get_attr(obj, attr, default=None):
    """Gets an attribute from an object safely."""
    return getattr(obj, attr, default)
 
def _get_top_n_by_group(df, group_by_col, n=5):
    """Helper to group, aggregate, sort, and return the top N rows."""
    if df.empty or group_by_col not in df.columns:
        return []
    grouped = (
        df.groupby(group_by_col, as_index=False)
        .agg({"Target Plan": "sum", "Est. Sales": "sum", "Ach. vs Target Plan": "sum"})
        .sort_values(
            by=["Ach. vs Target Plan", "Target Plan", "Est. Sales"],
            ascending=[True, False, True]
        )
    )
    return grouped.head(n).to_dict(orient="records") or []
 
# --- Core Data Processing Functions ---
def get_account_list(scenario_result, user_info):
    """Extracts a unique list of accounts from user_info."""
    try:
        soldto_data = user_info.sold_to_account_list
        account_set = set()
        for item in soldto_data:
            accounts = item.get("accounts", [])
            if accounts:  # only if accounts exist
                for acc in accounts:
                    account_set.add((acc["account_cd"], acc["account_nm"]))
        account_list = [
            {"account_cd": cd, "account_nm": nm} for cd, nm in account_set
        ]
        scenario_result.account_list = account_list
    except Exception as e:
        logging.error(f"Failed: {e}", exc_info=True)
        scenario_result.account_list = []
    scenario_result.CUR = user_info.user_currency
    return scenario_result
 
def process_sales_data(scenario_result, user_info, gscm_df, material_df):
    """Processes sales data to get totals, top accounts, and top product groups."""
    try:
        # Prepare filters
        account_cd_list = [str(acc["account_cd"]).strip() for acc in scenario_result.account_list]
        account_name_map = {str(acc["account_cd"]).strip(): acc["account_nm"] for acc in scenario_result.account_list}
        user_sales_org = str(safe_get_attr(user_info, "user_sales_org", "")).strip()
        auth_product_grp = list(safe_get_attr(user_info, "auth_product_grp", []) or [])
        # --- Grand Total & Top 5 Accounts (from GSCM.csv) ---
        if not gscm_df.empty:
            gscm_filtered = gscm_df[
                (gscm_df["GSCM Account"].isin(account_cd_list)) &
                (gscm_df["SALES_ORG_CD"] == user_sales_org)
            ].copy()
            gscm_filtered = gscm_filtered.drop(columns=["SALES_ORG_CD"])
            top5_accounts = gscm_filtered.head(5).to_dict(orient="records") or []
            # Add account name
            for acc in top5_accounts:
                acc_cd = str(acc.get("GSCM Account"))
                acc["GSCM Account Name"] = account_name_map.get(acc_cd)
           
            scenario_result.top5_accounts = top5_accounts
 
            if not gscm_filtered.empty:
                grand_total = gscm_filtered.select_dtypes(include='number').sum()
                scenario_result.total_target = grand_total.get("Target Plan")
                scenario_result.total_est_sales = grand_total.get("Est. Sales")
                scenario_result.total_ach_rate = grand_total.get("Ach. vs Target Plan")
                scenario_result.total_billing = grand_total.get("Billing")
                scenario_result.total_yoy = grand_total.get("Growth(%) Y-1")
        # --- Top 5 Product Groups (from Material.csv) ---
        if not material_df.empty:
            mat_filtered = material_df[
                (material_df["GSCM Account"].isin(account_cd_list)) &
                (material_df["SALES_ORG_CD"] == user_sales_org) &
                (material_df["GSCM P/Group"].isin(auth_product_grp))
            ].copy()
            scenario_result.top5_prdgs = _get_top_n_by_group(mat_filtered, "GSCM P/Group", 5)
    except Exception as e:
        logging.error(f"Failed: {e}", exc_info=True)
    # Ensure defaults are set
    for attr, default in [
        ("top5_accounts", []), ("top5_prdgs", []), ("total_target", None),
        ("total_est_sales", None), ("total_ach_rate", None),
        ("total_billing", None), ("total_yoy", None)
    ]:
        if not hasattr(scenario_result, attr):
            setattr(scenario_result, attr, default)
    return scenario_result
 
def fetch_operational_issues(scenario_result, rdb):
    """Fetches counts for various operational issues."""
    try:
        account_cd_list = [str(acc["account_cd"]).strip() for acc in scenario_result.account_list]
        rdd_date = scenario_result.local_time.strftime("%Y%m%d")
        today_date = scenario_result.local_time.strftime("%Y-%m-%d")
        scenario_result.today_date = today_date
        # Batched call for all operational issues for all authorized accounts
        all_issue_counts = safe_db_call(
            rdb.count_operational_issues_batch,
            account_cd_list,
            'account',
            rdd_date,
            today_date
        ) or {}
        # Aggregate the counts
        scenario_result.cnt_cred_block = sum(d.get("cnt_cred_block", 0) for d in all_issue_counts.values())
        scenario_result.cnt_del_block = sum(d.get("cnt_del_block", 0) for d in all_issue_counts.values())
        scenario_result.cnt_unconf_delay = sum(d.get("cnt_unconf_delay", 0) for d in all_issue_counts.values())
        scenario_result.cnt_do_delay = sum(d.get("cnt_do_delay", 0) for d in all_issue_counts.values())
        scenario_result.cnt_incomplete = sum(d.get("cnt_incomplete", 0) for d in all_issue_counts.values())
    except Exception as e:
        logging.error(f"Failed: {e}", exc_info=True)
        # Set defaults on failure
        for attr in ["cnt_cred_block", "cnt_del_block", "cnt_unconf_delay", "cnt_do_delay", "cnt_incomplete"]:
            setattr(scenario_result, attr, None)
    return scenario_result
 
def fetch_batched_order_amounts(scenario_result, rdb):
    """Fetches order amounts for top 5 accounts and product groups in batches."""
    try:
        today = scenario_result.local_time
        start_month = today.replace(day=1).strftime("%Y%m%d")
        next_month = (today.replace(day=1) + relativedelta(months=1)).strftime("%Y%m%d")
        after_next_month = (today.replace(day=1) + relativedelta(months=2)).strftime("%Y%m%d")
        # --- Process Accounts ---
        top_accounts_cds = [str(acc.get("GSCM Account", "")).strip() for acc in scenario_result.top5_accounts]
        # Batch DB calls for top accounts
        billed_by_acc = safe_db_call(rdb.get_ord_amt_account, top_accounts_cds, start_month, next_month) or {}
        open_by_acc = safe_db_call(rdb.get_ord_open_account, top_accounts_cds, start_month, next_month) or {}
        blocked_by_acc = safe_db_call(rdb.get_ord_block_account, top_accounts_cds, next_month) or {}
        upcoming_by_acc = safe_db_call(rdb.get_ord_upcoming_account, top_accounts_cds, next_month, after_next_month) or {}
        for i, acc_cd in enumerate(top_accounts_cds, start=1):
            setattr(scenario_result, f"top{i}_account_order", {
                "billed_order": billed_by_acc.get(acc_cd),
                "open_order": open_by_acc.get(acc_cd),
                "blocked_order": blocked_by_acc.get(acc_cd),
                "upcoming_order": upcoming_by_acc.get(acc_cd),
            })
        # --- Process Product Groups ---
        top_prdgs_cds = [str(p.get("GSCM P/Group", "")).strip() for p in scenario_result.top5_prdgs]
        billed_by_prdg = safe_db_call(rdb.get_ord_amt_prdg, top_prdgs_cds, start_month, next_month) or {}
        open_by_prdg = safe_db_call(rdb.get_ord_open_prdg, top_prdgs_cds, start_month, next_month) or {}
        blocked_by_prdg = safe_db_call(rdb.get_ord_block_prdg, top_prdgs_cds, next_month) or {}
        upcoming_by_prdg = safe_db_call(rdb.get_ord_upcoming_prdg, top_prdgs_cds, next_month, after_next_month) or {}
        for i, prdg_cd in enumerate(top_prdgs_cds, start=1):
            setattr(scenario_result, f"top{i}_prdg_order", {
                "billed_order": billed_by_prdg.get(prdg_cd),
                "open_order": open_by_prdg.get(prdg_cd),
                "blocked_order": blocked_by_prdg.get(prdg_cd),
                "upcoming_order": upcoming_by_prdg.get(prdg_cd),
            })
    except Exception as e:
        logging.error(f"Failed: {e}", exc_info=True)
    # Ensure defaults are set
    for i in range(1, 6):
        for attr_template in ["top{i}_account_order", "top{i}_prdg_order"]:
            attr_name = attr_template.format(i=i)
            if not hasattr(scenario_result, attr_name):
                setattr(scenario_result, attr_name, {})
    return scenario_result

def process_detailed_breakdowns(scenario_result, user_info, material_df):
    """Processes material data to find top models and accounts for various slices."""
    try:
        account_name_map = {str(acc["account_cd"]).strip(): acc["account_nm"] for acc in scenario_result.account_list}
        user_sales_org = str(safe_get_attr(user_info, "user_sales_org", "")).strip()
        auth_product_grp = list(safe_get_attr(user_info, "auth_product_grp", []) or [])
        account_cd_list = [str(acc["account_cd"]).strip() for acc in scenario_result.account_list]
        # --- Top Models for each Top Account ---
        for i, acc in enumerate(scenario_result.top5_accounts, start=1):
            selected_account = str(acc.get("GSCM Account", "")).strip()
            df_acc = material_df[
                (material_df["GSCM Account"] == selected_account) &
                (material_df["SALES_ORG_CD"] == user_sales_org) &
                (material_df["GSCM P/Group"].isin(auth_product_grp))
            ].copy()
            top_prdg = _get_top_n_by_group(df_acc, "GSCM P/Group", 1)
            setattr(scenario_result, f"top{i}_account_top1_prdg", top_prdg)
            top_prdg_name = top_prdg[0].get("GSCM P/Group") if top_prdg else None
            df_model = df_acc[df_acc["GSCM P/Group"] == top_prdg_name] if top_prdg_name else pd.DataFrame()
            setattr(scenario_result, f"top{i}_account_top1_prdg_top5_model", _get_top_n_by_group(df_model, "Material", 5))
        # --- Top Models and Accounts for each Top Product Group ---
        for i, prdg in enumerate(scenario_result.top5_prdgs, start=1):
            selected_prdg = str(prdg.get("GSCM P/Group", "")).strip()
            df_prdg = material_df[
                (material_df["GSCM Account"].isin(account_cd_list)) &
                (material_df["SALES_ORG_CD"] == user_sales_org) &
                (material_df["GSCM P/Group"] == selected_prdg)
            ].copy()
            # Top models for this product group
            top_prod = _get_top_n_by_group(df_prdg, "GSCM Product", 1)
            setattr(scenario_result, f"top{i}_prdg_top1_prd", top_prod)
            top_prod_name = top_prod[0].get("GSCM Product") if top_prod else None
            df_model = df_prdg[df_prdg["GSCM Product"] == top_prod_name] if top_prod_name else pd.DataFrame()
            setattr(scenario_result, f"top{i}_prdg_top1_prd_top5_model", _get_top_n_by_group(df_model, "Material", 5))
            # Top accounts for this product group
            top5_accounts = _get_top_n_by_group(df_prdg, "GSCM Account", 5)
            # Add account name to each account
            for acc in top5_accounts:
                acc_cd = str(acc.get("GSCM Account", "")).strip()
                acc["GSCM Account Name"] = account_name_map.get(acc_cd, "")
            setattr(scenario_result, f"top{i}_prdg_top5_account", top5_accounts)
    except Exception as e:
        logging.error(f"Failed: {e}", exc_info=True)
    # Ensure defaults are set
    for i in range(1, 6):
        for attr_template in [
            "top{i}_account_top1_prdg", "top{i}_account_top1_prdg_top5_model",
            "top{i}_prdg_top1_prd", "top{i}_prdg_top1_prd_top5_model", "top{i}_prdg_top5_account"
        ]:
            attr_name = attr_template.format(i=i)
            if not hasattr(scenario_result, attr_name):
                setattr(scenario_result, attr_name, [])
    return scenario_result
 
def convert_models_to_order_details(scenario_result, rdb):
    """Converts lists of models into detailed order/issue data using batch DB calls."""
    try:
        today = scenario_result.local_time
        start_month = today.replace(day=1).strftime("%Y%m%d")
        next_month = (today.replace(day=1) + relativedelta(months=1)).strftime("%Y%m%d")
        after_next_month = (today.replace(day=1) + relativedelta(months=2)).strftime("%Y%m%d")
        rdd_date = today.strftime("%Y%m%d")
        today_date = today.strftime("%Y-%m-%d")
        # Gather all unique materials from all model lists
        all_materials = set()
        source_attrs = [f"top{i}_account_top1_prdg_top5_model" for i in range(1, 6)] + \
                        [f"top{i}_prdg_top1_prd_top5_model" for i in range(1, 6)]
        for attr in source_attrs:
            for model in safe_get_attr(scenario_result, attr, []):
                if material_code := str(model.get("Material", "")).strip():
                    all_materials.add(material_code)
        materials_list = list(all_materials)
        if not materials_list:
            return scenario_result
        billed_orders = safe_db_call(rdb.get_ord_amt_material, materials_list, start_month, next_month) or {}
        open_orders = safe_db_call(rdb.get_ord_open_material, materials_list, start_month, next_month) or {}
        blocked_orders = safe_db_call(rdb.get_ord_block_material, materials_list, next_month) or {}
        upcoming_orders = safe_db_call(rdb.get_ord_upcoming_material, materials_list, start_month, next_month) or {}
 
        all_issue_counts = safe_db_call(
            rdb.count_operational_issues_batch,
            materials_list,
            'model_cd',
            rdd_date,
            today_date
        ) or {}
 
        # Map batched results back to the original structure
        for i in range(1, 6):
            for base_attr in ["account_top1_prdg_top5_model", "prdg_top1_prd_top5_model"]:
                input_attr = f"top{i}_{base_attr}"
                output_attr = f"{input_attr}_order"
                order_details = []
                for model in safe_get_attr(scenario_result, input_attr, []):
                    mat = str(model.get("Material", "")).strip()
                    mat_issues = all_issue_counts.get(mat, [mat, 0, 0, 0, 0, 0])
                    if not mat: continue
                    order_details.append({
                        "Material": mat,
                        "billed_order": billed_orders.get(mat), "open_order": open_orders.get(mat),
                        "blocked_order": blocked_orders.get(mat), "upcoming_order": upcoming_orders.get(mat),
                        "cnt_cred_block": mat_issues[1], "cnt_del_block": mat_issues[2],
                        "cnt_unconf_delay": mat_issues[3], "cnt_do_delay": mat_issues[4],
                        "cnt_incomplete": mat_issues[5],
                    })
                setattr(scenario_result, output_attr, order_details)
    except Exception as e:
        logging.error(f"Failed: {e}", exc_info=True)
    # Ensure defaults are set
    for i in range(1, 6):
        for attr_template in ["top{i}_account_top1_prdg_top5_model_order", "top{i}_prdg_top1_prd_top5_model_order"]:
            attr_name = attr_template.format(i=i)
            if not hasattr(scenario_result, attr_name):
                setattr(scenario_result, attr_name, [])
    return scenario_result

def build_SOSE00(scenario_result):
    return {
        "top5_accounts": safe_get_attr(scenario_result, "top5_accounts", []) or []
    }
 
def build_SODE00(scenario_result):
    base = {
        "top5_accounts": safe_get_attr(scenario_result, "top5_accounts", []) or [],
        "CUR": safe_get_attr(scenario_result, "CUR", None),
    }
    # orders and product info
    for i in range(1, 6):
        base[f"top{i}_account_order"] = safe_get_attr(scenario_result, f"top{i}_account_order", {}) or {}
        base[f"top{i}_account_top1_prdg"] = safe_get_attr(scenario_result, f"top{i}_account_top1_prdg", []) or []
        base[f"top{i}_account_top1_prdg_top5_model"] = safe_get_attr(scenario_result, f"top{i}_account_top1_prdg_top5_model", []) or []
    return base
 
def build_PGSE00(scenario_result):
    return {
        "top5_prdgs": safe_get_attr(scenario_result, "top5_prdgs", []) or []
    }
 
def build_PGDE00(scenario_result):
    base = {
        "top5_prdgs": safe_get_attr(scenario_result, "top5_prdgs", []) or [],
        "CUR": safe_get_attr(scenario_result, "CUR", None),
    }
    for i in range(1, 6):
        base[f"top{i}_prdg_order"] = safe_get_attr(scenario_result, f"top{i}_prdg_order", {}) or {}
        base[f"top{i}_prdg_top1_prd"] = safe_get_attr(scenario_result, f"top{i}_prdg_top1_prd", []) or []
        base[f"top{i}_prdg_top1_prd_top5_model"] = safe_get_attr(scenario_result, f"top{i}_prdg_top1_prd_top5_model", []) or []
        base[f"top{i}_prdg_top5_account"] = safe_get_attr(scenario_result, f"top{i}_prdg_top5_account", []) or []
    return base
 
def build_MODE00(scenario_result):
    base = {"CUR": safe_get_attr(scenario_result, "CUR", None),}
    for i in range(1, 6):
        base[f"top{i}_account_top1_prdg_top5_model_order"] = safe_get_attr(scenario_result, f"top{i}_account_top1_prdg_top5_model_order", []) or []
        base[f"top{i}_prdg_top1_prd_top5_model_order"] = safe_get_attr(scenario_result, f"top{i}_prdg_top1_prd_top5_model_order", []) or []
    return base
 
def build_OPTR00(scenario_result):
    return {
        "total_est_sales": safe_get_attr(scenario_result, "total_est_sales", None),
        "total_billed_order": safe_get_attr(scenario_result, "total_billed_order", None),
        "total_open_order": safe_get_attr(scenario_result, "total_open_order", None),
        "total_blocked_order": safe_get_attr(scenario_result, "total_blocked_order", None),
        "total_upcoming_order": safe_get_attr(scenario_result, "total_upcoming_order", None),
        "cnt_cred_block": safe_get_attr(scenario_result, "cnt_cred_block", None),
        "cnt_del_block": safe_get_attr(scenario_result, "cnt_del_block", None),
        "cnt_unconf_delay": safe_get_attr(scenario_result, "cnt_unconf_delay", None),
        "cnt_do_delay": safe_get_attr(scenario_result, "cnt_do_delay", None),
        "cnt_incomplete": safe_get_attr(scenario_result, "cnt_incomplete", None),
        "CUR": safe_get_attr(scenario_result, "CUR", None),
    }
 
def build_page(scenario_result):
    # LAPA00 is a top-level summary, so it should always be built.
    scenario_result.LAPA00 = build_LAPA00(scenario_result)
    # Check if top5_accounts is not empty before building related pages
    if safe_get_attr(scenario_result, "top5_accounts", []):
        scenario_result.SOSE00 = build_SOSE00(scenario_result)
        scenario_result.SODE00 = build_SODE00(scenario_result)
    else:
        # Explicitly set to an empty dictionary or a default value
        scenario_result.SOSE00 = {}
        scenario_result.SODE00 = {}
    # Check if top5_prdgs is not empty before building related pages
    if safe_get_attr(scenario_result, "top5_prdgs", []):
        scenario_result.PGSE00 = build_PGSE00(scenario_result)
        scenario_result.PGDE00 = build_PGDE00(scenario_result)
    else:
        # Explicitly set to an empty dictionary or a default value
        scenario_result.PGSE00 = {}
        scenario_result.PGDE00 = {}
    # MODE00 is a combination of both; execute only if either list is not empty
    if safe_get_attr(scenario_result, "top5_accounts", []) or safe_get_attr(scenario_result, "top5_prdgs", []):
        scenario_result.MODE00 = build_MODE00(scenario_result)
    else:
        scenario_result.MODE00 = {}
    # OPTR00 is a top-level summary, so it should always be built.
    scenario_result.OPTR00 = build_OPTR00(scenario_result)
    return scenario_result
 
def execute_scenario(scenario_result, user_info):
    """
    Main orchestrator to execute the data processing pipeline efficiently and safely.
    """
    logging.info("Starting scenario execution.")
 
    # --- 1. Setup: Load data and initialize connections ---
    data_path = os.path.join(CONFIG["project_root"], CONFIG["data_dir"])
    gscm_df = safe_read_csv(os.path.join(data_path, "GSCM.csv"), dtype={"SALES_ORG_CD": str, "GSCM Account": str})
    material_df = safe_read_csv(os.path.join(data_path, "Material.csv"), dtype={"SALES_ORG_CD": str, "GSCM Account": str})
    rdb = RDB()
   
    # --- 2. Sequential Data Processing ---
    try:
        scenario_result = get_account_list(scenario_result, user_info)
        # Combines multiple file-based operations
        scenario_result = process_sales_data(scenario_result, user_info, gscm_df, material_df)
        # Combines multiple detail-level operations
        scenario_result = process_detailed_breakdowns(scenario_result, user_info, material_df)
        # Combines multiple DB-based operations
        scenario_result = fetch_operational_issues(scenario_result, rdb)
        scenario_result = fetch_batched_order_amounts(scenario_result, rdb)
        scenario_result = convert_models_to_order_details(scenario_result, rdb)
        # Build final output pages
        scenario_result = build_page(scenario_result)
    except Exception as e:
        logging.critical(f"Unhandled exception during scenario execution: {e}", exc_info=True)
        # Ensure a safe but empty result is returned
        scenario_result = build_page(scenario_result) # Build with whatever data exists
    logging.info("Scenario execution finished.")
    return scenario_result