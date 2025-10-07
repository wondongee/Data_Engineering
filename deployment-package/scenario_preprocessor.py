import os, sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
sys.path.append(PROJECT_ROOT)
 
import uuid
import json
import time
import datetime
import pandas as pd
import time
import traceback
import copy
 
from typing import List, Optional
from pydantic import BaseModel
 
from src.modules.standalone_functions.user_manager import UserManager
from src.modules.agents.analysis.orm.oracle import ORACLE
from src.modules.agents.analysis.orm.postgres import RDB
from src.modules.agents.query.t2a.query_t2a import get_bim_data
from src.modules.agents.analysis.utils.action_registry import execute_scenario
 
QUERY_TEMPLATE_DIR = os.path.join(
    PROJECT_ROOT, "src", "modules", "agents", "query", "t2a", "templates"
)
 
TEMP_STORE_DIR = os.path.join(
    PROJECT_ROOT, "src", "modules", "agents", "analysis", "utils"
)
 
###### CONFIGS
 
###### BASE MODELS
class ScenarioResult(BaseModel):
    execution_id: str
    sender_name: str
    user_id: str
    chatroom_id: str
    today_date: Optional[str] = None
    currency: Optional[str] = None
 
    # 담당 account list
    # [{'account_cd': '5003750', 'account_nm': 'MEDIA MARKT'}, {'account_cd': '5003750', 'account_nm': 'MEDIA MARKT'}, ...]
    account_list: Optional[List[dict]] = None
    # Grand Total
    total_target: Optional[int] = None
    total_est_sales: Optional[int] = None
    total_ach_rate: Optional[int] = None
    total_billing: Optional[int] = None
    total_yoy: Optional[int] = None
    # [{'GSCM Account': '5009095', 'Target Plan': 0.0, 'Est. Sales': 0.0, 'Ach. vs Target Plan': 0.0}, {'GSCM Account': '5999999', 'Target Plan': 0.0, 'Est. Sales': 0.0, 'Ach. vs Target Plan': 0.0}, ...]
    top5_accounts: Optional[List[dict]] = None
    # [{'GSCM P/Group': 'VC', 'Target Plan': 2507985, 'Est. Sales': 0, 'Ach. vs Target Plan': 0.0}, {'GSCM P/Group': 'MONITOR', 'Target Plan': 11597026, 'Est. Sales': 429100, 'Ach. vs Target Plan': 6.6}, ...]
    top5_prdgs: Optional[List[dict]] = None
 
    # Operation Issues
    cnt_cred_block: Optional[int] = None
    cnt_del_block: Optional[int] = None
    cnt_unconf_delay: Optional[int] = None
    cnt_do_delay: Optional[int] = None
    cnt_incomplete: Optional[int] = None
 
    # Order Amount
    # {"billed_order": 0, "open_order": 0, "bloked_order": 0, "upcoming_order": 0}
    top1_account_order: Optional[dict] = None
    top2_account_order: Optional[dict] = None
    top3_account_order: Optional[dict] = None
    top4_account_order: Optional[dict] = None
    top5_account_order: Optional[dict] = None
 
    # Top5 Account에 대한 Top1 Product Group
    # [{'GSCM P/Group': 'VC', 'Target Plan': 34323, 'Est. Sales': 0, 'Ach. vs Target Plan': 0.0}]
    top1_account_top1_prdg: Optional[List[dict]] = None
    top2_account_top1_prdg: Optional[List[dict]] = None
    top3_account_top1_prdg: Optional[List[dict]] = None
    top4_account_top1_prdg: Optional[List[dict]] = None
    top5_account_top1_prdg: Optional[List[dict]] = None
 
    # Top1 Product Group에 대한 Top5 Material
    # [{'Material': 'WF45B6300AP/US', 'Target Plan': 1346, 'Est. Sales': 0, 'Ach. vs Target Plan': 0.0}, ...]
    top1_account_top1_prdg_top5_model: Optional[List[dict]] = None
    top2_account_top1_prdg_top5_model: Optional[List[dict]] = None
    top3_account_top1_prdg_top5_model: Optional[List[dict]] = None
    top4_account_top1_prdg_top5_model: Optional[List[dict]] = None
    top5_account_top1_prdg_top5_model: Optional[List[dict]] = None
 
    # Order Amount
    # {"billed_order": 0, "open_order": 0, "bloked_order": 0, "upcoming_order": 0}
    top1_prdg_order: Optional[dict] = None
    top2_prdg_order: Optional[dict] = None
    top3_prdg_order: Optional[dict] = None
    top4_prdg_order: Optional[dict] = None
    top5_prdg_order: Optional[dict] = None
 
    # Top5 Product Group 대한 Top1 Product
    # [{'GSCM P/Group': 'QLED', 'Target Plan': 34323, 'Est. Sales': 0, 'Ach. vs Target Plan': 0.0}]
    top1_prdg_top1_prd: Optional[List[dict]] = None
    top2_prdg_top1_prd: Optional[List[dict]] = None
    top3_prdg_top1_prd: Optional[List[dict]] = None
    top4_prdg_top1_prd: Optional[List[dict]] = None
    top5_prdg_top1_prd: Optional[List[dict]] = None
 
    # Top1 Product에 대한 Top5 Material
    # [{'Material': 'WF45B6300AP/US', 'Target Plan': 1346, 'Est. Sales': 0, 'Ach. vs Target Plan': 0.0}, ...]
    top1_prdg_top1_prd_top5_model: Optional[List[dict]] = None
    top2_prdg_top1_prd_top5_model: Optional[List[dict]] = None
    top3_prdg_top1_prd_top5_model: Optional[List[dict]] = None
    top4_prdg_top1_prd_top5_model: Optional[List[dict]] = None
    top5_prdg_top1_prd_top5_model: Optional[List[dict]] = None
 
    # Top5 Product Group 대한 Top5 Account
    # [{'GSCM Account': '5009095', 'Target Plan': 34323, 'Est. Sales': 0, 'Ach. vs Target Plan': 0.0}]
    top1_prdg_top5_account: Optional[List[dict]] = None
    top2_prdg_top5_account: Optional[List[dict]] = None
    top3_prdg_top5_account: Optional[List[dict]] = None
    top4_prdg_top5_account: Optional[List[dict]] = None
    top5_prdg_top5_account: Optional[List[dict]] = None
 
    # Material에 대한 Order Detail (5개씩 총 50개)
    # [{'Material': 'WF45B6300AP/US', 'billed_order': 0, 'open_order': 0, 'bloked_order': 0, 'upcoming_order': 0, 'cnt_cred_block': 0, 'cnt_del_block': 0, 'cnt_unconf_delay': 0, 'cnt_do_delay': 0, 'cnt_incomplete': 0}, ...]
    top1_account_top1_prdg_top5_model_order: Optional[List[dict]] = None
    top2_account_top1_prdg_top5_model_order: Optional[List[dict]] = None
    top3_account_top1_prdg_top5_model_order: Optional[List[dict]] = None
    top4_account_top1_prdg_top5_model_order: Optional[List[dict]] = None
    top5_account_top1_prdg_top5_model_order: Optional[List[dict]] = None
    top1_prdg_top1_prd_top5_model_order: Optional[List[dict]] = None
    top2_prdg_top1_prd_top5_model_order: Optional[List[dict]] = None
    top3_prdg_top1_prd_top5_model_order: Optional[List[dict]] = None
    top4_prdg_top1_prd_top5_model_order: Optional[List[dict]] = None
    top5_prdg_top1_prd_top5_model_order: Optional[List[dict]] = None
 
    # Operation Tracker
    total_billed_order: Optional[int] = None
    total_open_order: Optional[int] = None
    total_blocked_order: Optional[int] = None
    total_upcoming_order: Optional[int] = None
 
    ####### LAPA00
    # LAPA00 = {"sender_name": "", "today_date": "", "total_target": 0, "total_est_sales": 0, "total_ach_rate": 0, "total_billing": 0, "total_yoy": 0, "top5_accounts": [], "top5_prdgs": [], "account_list": [], "cnt_cred_block": 0, "cnt_del_block": 0, "cnt_unconf_delay": 0, "cnt_do_delay": 0, "cnt_incomplete": 0}
    LAPA00: Optional[dict] = None
 
    ####### SOSE00
    # SOSE00 = {"top5_accounts": []}
    SOSE00: Optional[dict] = None
 
    ####### SODE00
    # SODE00 = {"top5_accounts": [], "top1_account_order": [], "top2_account_order": [], "top3_account_order": [], "top4_account_order": [], "top5_account_order": [], "top1_account_top1_prdg": [], "top2_account_top1_prdg": [], "top3_account_top1_prdg": [], "top4_account_top1_prdg": [], "top5_account_top1_prdg": [], "top1_account_top1_prdg_top5_model": [], "top2_account_top1_prdg_top5_model": [], "top3_account_top1_prdg_top5_model": [], "top4_account_top1_prdg_top5_model": [], "top5_account_top1_prdg_top5_model": []}
    SODE00: Optional[dict] = None
 
    ####### PGSE00
    # PGSE00 = {"top5_prdgs": []}
    PGSE00: Optional[dict] = None
 
    ####### PGDE00
    # PGDE00 = {"top5_prdgs": [], "top1_prdg_order": [], "top2_prdg_order": [], "top3_prdg_order": [], "top4_prdg_order": [], "top5_prdg_order": [], "top1_prdg_top1_prd": [], "top2_prdg_top1_prd": [], "top3_prdg_top1_prd": [], "top4_prdg_top1_prd": [], "top5_prdg_top1_prd": [], "top1_prdg_top1_prdg_top5_model": [], "top2_prdg_top1_prdg_top5_model": [], "top3_prdg_top1_prdg_top5_model": [], "top4_prdg_top1_prdg_top5_model": [], "top5_prdg_top1_prdg_top5_model": [], "top1_prdg_top5_account": [], "top2_prdg_top5_account": [], "top3_prdg_top5_account" [], "top4_prdg_top5_account": [], "top5_prdg_top5_account": []}
    PGDE00: Optional[dict] = None
 
    ####### MODE00
    # MODE00 = {"top1_account_top1_prdg_top5_model_order": [], "top2_account_top1_prdg_top5_model_order": [], "top3_account_top1_prdg_top5_model_order": [], "top4_account_top1_prdg_top5_model_order", "top5_account_top1_prdg_top5_model_order": [], "top1_prdg_top1_prdg_top5_model_order": [], "top2_prdg_top1_prdg_top5_model_order": [], "top3_prdg_top1_prdg_top5_model_order": [], "top4_prdg_top1_prdg_top5_model_order": [], "top5_prdg_top1_prdg_top5_model_order": []}
    MODE00: Optional[dict] = None
 
    ####### OPTR00
    # OPTR00 = {"total_est_sales": 0, "total_billed_order": 0, "total_open_order": 0, "total_blocked_order": 0, "total_upcoming_order": 0, "cnt_cred_block": 0, "cnt_del_block": 0, "cnt_unconf_delay": 0, "cnt_do_delay": 0, "cnt_incomplete": 0}
    OPTR00: Optional[dict] = None
 
 
    class Config:
        extra = 'allow'
 
# --------------------- 1차 csv 데이터 적재 --------------------------- #
 
###### HELPER FUNCTIONS
 
def prepare_params_for_sales_org(sales_org_cd, local_currency):
    # 수정 필요
    today_date = datetime.datetime.now()
    return {
        "{today_date/YYYY.MM}": today_date.strftime("%Y.%m"),
        "{today_date/YYYYMM}": today_date.strftime("%Y%m"),
        "{CUR}": local_currency,
        "{sales_org_cd}": sales_org_cd,
    }
 
def load_query(template_name):
    file_path = os.path.join(QUERY_TEMPLATE_DIR, template_name)
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)
 
def execute_query(template_name, params):
    query_template = load_query(template_name)
    final_query = replace_placeholders(copy.deepcopy(query_template), params)
    query_json = json.dumps(final_query, ensure_ascii=False)
    print(query_json)
    longform = get_bim_data(query_json)
    print(longform)
    return json.loads(longform)

def parse_longform(longform):
    meta_str = longform.get("meta", {}).get("content", "").lstrip("\ufeff").strip()
    # Return empty if meta_str is not valid
    if not meta_str or meta_str in ["{}", "[]"]:
        return [], []
    try:
        meta_content = json.loads(meta_str)
    except json.JSONDecodeError:
        print("Invalid JSON in meta.content:", repr(meta_str))
        return [], []
    headers = [h.replace("\xa0", " ") for h in meta_content.get("headers", [])]
    rows = meta_content.get("rows", [])
    return headers, rows
 
def filter_and_sort_rows(headers, rows):
    idx_target = headers.index("Target Plan")
    idx_ach = headers.index("Ach. vs Target Plan")
    idx_est = headers.index("Est. Sales")
    # Filter rows
    filtered_rows = [r for r in rows if r[idx_target] != 0 and r[idx_ach] < 100]
 
    # Fallback: if no rows found
    if not filtered_rows:
        filtered_rows = [r for r in rows if r[idx_ach] < 100]
 
    # Sort ascending by achievement
    sorted_rows = sorted(filtered_rows, key=lambda r: (r[idx_ach], -r[idx_target], r[idx_est]))
    return sorted_rows
 
def replace_placeholders(obj, params):
    if isinstance(obj, dict):
        return {k: replace_placeholders(v, params) for k, v in obj.items()}
    elif isinstance(obj, list):
        new_list = []
        for item in obj:
            replaced = replace_placeholders(item, params)
            if isinstance(replaced, list) and any(ph in str(item) for ph in params.keys()):
                new_list.extend(replaced)
            else:
                new_list.append(replaced)
        return new_list
    elif isinstance(obj, str):
        return params.get(obj, obj)
    else:
        return obj
 
###### MAIN EXECUTION FUNCTIONS
 
# Sales Org 별로 Oracle에서 target, est. sales, % 가져오기
def make_sales_org_pool():
    start_total = time.time()
    oracle = ORACLE()
    sales_org_list = oracle.fetch_sales_org_code()
    all_dfs = []
    for sales_org_cd in sales_org_list:
        start_time = time.time()  # start timing
        try:
            local_currency = oracle.fetch_currency(sales_org_cd)
            params = prepare_params_for_sales_org(sales_org_cd, local_currency)
            longform = execute_query("SALES_ORG.json", params)
            headers, rows = parse_longform(longform)
            sorted_rows = filter_and_sort_rows(headers, rows)
            # Create DataFrame
            df = pd.DataFrame(sorted_rows, columns=headers)
            # Add sales_org_cd as the first column
            df.insert(0, "SALES_ORG_CD", sales_org_cd)
            # 🔹 Convert to string for consistency
            for col in ["SALES_ORG_CD", "GSCM Account", "SoldTo"]:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            print(df)
            all_dfs.append(df)
            end_time = time.time()  # end timing
            elapsed = end_time - start_time
            print(f"Sales Org {sales_org_cd} processed in {elapsed:.2f} seconds")
        except Exception as e:
            end_time = time.time()
            elapsed = end_time - start_time
            print(f"Error processing Sales Org {sales_org_cd} after {elapsed:.2f} seconds")
            print(f"Exception: {e}")
            traceback.print_exc()  # prints full traceback
    if all_dfs:
        result_df = pd.concat(all_dfs, ignore_index=True)
    else:
        result_df = pd.DataFrame()  # empty DataFrame if no data processed
    # Save to CSV
    file_path = os.path.join(TEMP_STORE_DIR, "Material.csv")
    result_df.to_csv(file_path, index=False,)
   
    end_total = time.time()
    total_elapsed = end_total - start_total
    print(f"\nTotal time taken for make_sales_org_pool: {total_elapsed:.2f} seconds")
    return result_df
 
# Sales Org 별로 Oracle에서 total 가져오기
def make_sales_org_total_pool():
    start_total = time.time()
    oracle = ORACLE()
    sales_org_list = oracle.fetch_sales_org_code()
    all_dfs = []
    for sales_org_cd in sales_org_list:
        start_time = time.time()  # start timing
        try:
            local_currency = oracle.fetch_currency(sales_org_cd)
            params = prepare_params_for_sales_org(sales_org_cd, local_currency)
            longform = execute_query("SALES_TOTAL.json", params)
            headers, rows = parse_longform(longform)
            sorted_rows = filter_and_sort_rows(headers, rows)
            # Create DataFrame
            df = pd.DataFrame(sorted_rows, columns=headers)
            print(df)
            # Add sales_org_cd as the first column
            df.insert(0, "SALES_ORG_CD", sales_org_cd)
            # 🔹 Convert to string for consistency
            for col in ["SALES_ORG_CD", "GSCM Account"]:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            print(df)
            all_dfs.append(df)
            end_time = time.time()  # end timing
            elapsed = end_time - start_time
            print(f"Sales Org {sales_org_cd} processed in {elapsed:.2f} seconds")
        except Exception as e:
            end_time = time.time()
            elapsed = end_time - start_time
            print(f"Error processing Sales Org {sales_org_cd} after {elapsed:.2f} seconds")
            print(f"Exception: {e}")
            traceback.print_exc()  # prints full traceback
    if all_dfs:
        result_df = pd.concat(all_dfs, ignore_index=True)
    else:
        result_df = pd.DataFrame()  # empty DataFrame if no data processed
    # Save to CSV
    file_path = os.path.join(TEMP_STORE_DIR, "GSCM.csv")
    result_df.to_csv(file_path, index=False,)
 
    end_total = time.time()
    total_elapsed = end_total - start_total
    print(f"\nTotal time taken for make_sales_org_pool: {total_elapsed:.2f} seconds")
    return result_df
 
 
# --------------------- 2차 Postgres 데이터 적재 --------------------------- #
 
###### HELPER FUNCTIONS
 
def calc_local_time(server_time: float, user_sales_org: str) -> datetime.datetime:
    """
    Convert server time (epoch seconds) to local time given a GMT offset.
 
    Args:
        server_time (float): Epoch time from time.time(), assumed UTC-based.
        gmt_offset_str (str): GMT offset as a string (e.g., "-9", "+5.5").
 
    Returns:
        datetime.datetime: Local datetime adjusted by the GMT offset.
    """
    gmt_offset_str = ORACLE().fetch_tz_info(user_sales_org)
    utc_time = datetime.datetime.utcfromtimestamp(server_time)
    offset_hours = float(gmt_offset_str)
    local_time = utc_time + datetime.timedelta(hours=offset_hours)
    return local_time
 
def execute_dair(scenario_result): # : ScenarioResult
 
    # USER INFO 불러오기
    user_manager = UserManager()
    user_info = user_manager.get_or_create_user_info(f"{scenario_result.user_id}", f"{scenario_result.user_id}", f"{scenario_result.execution_id}", "space")
     # Calculate Local Time
    server_time = time.time()
    local_time = calc_local_time(server_time=server_time, user_sales_org=user_info.user_sales_org)
    scenario_result.server_time = server_time #datetime.datetime
    scenario_result.local_time = local_time #datetime.datetime
 
    execute_scenario(scenario_result, user_info)
    return scenario_result
 
def save_scenario_result_to_db_rdb(scenario_result):
 
    # Map scenario_result fields to lowercase DB columns
    record = {
        "user_id": scenario_result.user_id,
        "lapa00": scenario_result.LAPA00,
        "sose00": scenario_result.SOSE00,
        "sode00": scenario_result.SODE00,
        "pgse00": scenario_result.PGSE00,
        "pgde00": scenario_result.PGDE00,
        "mode00": scenario_result.MODE00,
        "optr00": scenario_result.OPTR00,
    }
   
    rdb = RDB()
    rdb.save_result(record)
    
###### MAIN EXECUTION FUNCTIONS
 
def plan_execution():
    oracle = ORACLE()
    target_users = oracle.fetch_ai_users()
    # target_users = [('yk2002.yoon',)]
    # target_users = [('TEST.AAA',), ('doyeong3.kim',), ('sung011',), ('SSH_5503_NOR',), ('ARUM33_5503_NOR',), ('yang002.liu_3101',), ('SSSB_5503',), ('ORDERADMIN2_3101',), ('SH529.YOON2_5503',), ('TEST_W2W_USER',), ('TEST_LKJ',), ('SSSB_5503_TST3',), ('ALIM.YOO_5503',), ('JITAO',), ('3101_Q2C_W2W',), ('SSO5503',), ('YWBEST.PARK_5503',), ('LKJ_T_5503',), ('KYUJONG.CHOI_5503',), ('TAO.JI_5503_NOR',), ('bingze.lei',), ('b1.cao',), ('JEONG',), ('jaewoo.lee',), ('SH529.YOON_5503',), ('DEV17C',), ('JUNHO5503_50',), ('seokhoon.son_7103',), ('SB_5503',), ('yunghee.song',), ('YB_NEW_CHOI',), ('arum33.sim',), ('b1.cao_5503',), ('dy_3101',), ('EUNJU_5503',), ('ALIM.YOO_5503_THD',), ('TES.SHIM',), ('ALIM.YOO_5503_TEST',), ('ALIM.YOO_5503_NOR',), ('YB.NEW.CHOI',), ('DCK_5503',), ('SSSB_5503_TST',), ('michael1.lee01',), ('KSJEONG',), ('LKJ_5503',), ('HOSIK_5503',), ('JUNHO5503_TEST',), ('HOSIK_5503_NOR',), ('KYUJONG.CHOI_CU',), ('INSANG.HAN',), ('BRIAN_SIEL',), ('ORDERADMIN2_5503',), ('XL03.HE_5503_NOR',), ('YWBEST.PARK10',), ('ALIM.YOO_5503_CU',), ('joohyang.lee1',), ('JUNHO_SIEL',), ('ALIM.YOO_5503_HDI',), ('sh0125.kim',), ('KATE.LEE_3101_CU2',), ('song.kuk',), ('hayoug.chang',), ('chan5.park',), ('e.sull',), ('seonghee.cho',), ('sungjin.p2',), ('JY26.YANG_3101',), ('beyondn',), ('sungjin.p',), ('b1.cao_3101',), ('miseon2.kang',), ('albert1.kim',), ('ORDERADMIN_5503',), ('j30.kang',), ('domin.pyun',), ('aaa01',), ('KATE.LEE_3101_CU1',), ('INTIFUSER',), ('ilvolo',), ('pine0314',), ('MINHEE01LEE_S300',), ('alim.yoo',), ('ORDERADMIN_3101',), ('yk2002.yoon',), ('yang002.liu_AM',), ('bowstring.park02',), ('yl_samsung_test',), ('inbum.chong',), ('yj.pyo',), ('joonhwan.hur',), ('soyeone.park',), ('jaeyeon3.ryu',), ('swon1.shin',), ('jsjins.park',), ('kibumpwc.kim',), ('KATE.LEE_5503_SA',), ('SH.YUN_5503_SA',), ('gajanan.j',), ('p.thirumal',), ('karen.e',), ('SY00.HAN_5503_NOR',), ('a.haaf',), ('siegentest01',), ('kate.lee',), ('hooguen.baek',), ('j.gregory',), ('SY00.HAN_3101_NOR',), ('YK2002.YOON_NA',), ('CHO_CUST_02',), ('AGLOVER',), ('SW0523.LEE_SEA',), ('KYOUNGJUNE_3101',), ('SAM1.NAM',), ('AARONP',), ('KATE.LEE_3101_SA1',), ('A.COLACIOS',), ('KATE.LEE_3101_SA',), ('SUNGHUN_3101_NOR',), ('KS813.KIM',), ('3101_ADDISON_COURT_W2W',), ('JOOHYANG.LEE',), ('YJ0526.SEOK_SEA',)]
    print(target_users)
    for idx, user_tuple in enumerate(target_users, start=1):
        try:
            user_id = user_tuple[0]
            test = ScenarioResult(
                execution_id=str(uuid.uuid4()),
                sender_name=user_id,
                user_id=user_id,
                chatroom_id=str(uuid.uuid4())
            )
            scenario_result = execute_dair(test)
        except Exception as e:
            print(f"[ERROR] Failed to execute scenario for user {user_tuple}: {e}")
            continue  # skip to next user
        try:
            save_scenario_result_to_db_rdb(scenario_result)
        except Exception as e:
            print(f"[ERROR] Failed to save scenario result for user {user_id}: {e}")
            continue  # skip to next user
    print("[INFO] Plan execution completed.")
 
###### DEBUG
if __name__ == "__main__":
    uid = uuid.uuid4()
    test = ScenarioResult(
        execution_id = f"{uid}",
        sender_name = "hooguen",
        user_id = "hooguen.baek",
        chatroom_id = "99998888"
    )
   
    # execute_dair(test)
 
    # execute_dair(None)
    plan_execution()
 
    # make_sales_org_total_pool()
    pass
 
######## 데이터를 하나씩 처리하기 위한 모듈 ####
 
def make_sales_org_pool_one(sales_org_cd: str, window_id: str) -> pd.DataFrame:
   """Material(상세)용: sales_org_cd 하나만 처리해서 DataFrame 리턴"""
   start = time.time()
   oracle = ORACLE()
   try:
       local_currency = oracle.fetch_currency(sales_org_cd)
       params = prepare_params_for_sales_org(sales_org_cd, local_currency)
       longform = execute_query("SALES_ORG.json", params)
       headers, rows = parse_longform(longform)
       rows = filter_and_sort_rows(headers, rows)
       df = pd.DataFrame(rows, columns=headers)
       # 식별 컬럼 보강
       df.insert(0, "SALES_ORG_CD", str(sales_org_cd))
       for col in ("SALES_ORG_CD", "GSCM Account", "SoldTo"):
           if col in df.columns: df[col] = df[col].astype(str)
       return df
   except Exception as e:
       print(f"[make_sales_org_pool_one] {sales_org_cd} 실패: {e}")
       traceback.print_exc()
       return pd.DataFrame()
   finally:
       print(f"[make_sales_org_pool_one] {sales_org_cd} elapsed={time.time()-start:.2f}s")
       
def make_sales_org_total_pool_one(sales_org_cd: str, window_id: str) -> pd.DataFrame:
   """GSCM(합계)용: sales_org_cd 하나만 처리해서 DataFrame 리턴"""
   start = time.time()
   oracle = ORACLE()
   try:
       local_currency = oracle.fetch_currency(sales_org_cd)
       params = prepare_params_for_sales_org(sales_org_cd, local_currency)
       longform = execute_query("SALES_TOTAL.json", params)
       headers, rows = parse_longform(longform)
       rows = filter_and_sort_rows(headers, rows)
       df = pd.DataFrame(rows, columns=headers)
       df.insert(0, "SALES_ORG_CD", str(sales_org_cd))
       for col in ("SALES_ORG_CD", "GSCM Account"):
           if col in df.columns: df[col] = df[col].astype(str)
       return df
   except Exception as e:
       print(f"[make_sales_org_total_pool_one] {sales_org_cd} 실패: {e}")
       traceback.print_exc()
       return pd.DataFrame()
   finally:
       print(f"[make_sales_org_total_pool_one] {sales_org_cd} elapsed={time.time()-start:.2f}s")