import os, sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
sys.path.append(PROJECT_ROOT)
 
import uuid
import json
import time
import datetime
import pickle
 
from typing import List, Optional, Union
from pydantic import BaseModel
 
from src.modules.standalone_functions.user_manager import UserManager
from src.modules.agents.analysis.orm.oracle import ORACLE
from src.modules.agents.analysis.orm.postgres import RDB
 
"""
BACKGROUND CONTEXT:
- 매일 (토, 일 제외) 유저가 설정한 시각 1시간 이전에 모듈 실행해 REDIS에 Caching
- 매 시각 Scheduling 필요
DATA STORE:
- 시나리오 계산 결과값      --> dataclass (pkl)
- 유저 액션 Caching        --> REDIS
- 배치 로그 / 유저 로그     --> PSQL
STEPS:
1. INIT Ontlology
- 구조도에 따라 Scenario 구분 (화면 別)
- 모든 Scenario 온톨로지 초기화 後 실행
2. Execute DAI System
- 결과값 Caching to pkl
- 화면 別 선택 값에 따른 API 준비 (/analysis/api)
"""
 
"""
SCHEDULING 기획
1. 매 정각 make_user_pool() 실행
쿼리 한큐로 해결
1-1. Sales Org 別로 어떤 User Group이 AI_USE_YN 인지 판단 -> User Group Pool 정의
1-2. User Group 하위 User 들의 NOTI_YN (ZTHM_AI_WATCHLIST) 여부 판단 -> User Candidate 정의
1-3. User Candidate 中 설정 시각이 현재 기준 <= 60이면 대상으로 판단 -> Final User Pool 정의
2. 시나리오 실행 계획 수립
2-1. len(final_user_pool)이 주어졌을 때, 리소스 가용량 50% 범주에서 Threading?
"""
 
###### CONFIGS
 
###### BASE MODELS
class ScenarioResult(BaseModel):
    execution_id: str
    sender_name: str
    user_id: str
    chatroom_id: str
 
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
    # LAPA00 = {"sender_name": "", "total_target": 0, "total_est_sales": 0, "total_ach_rate": 0, "total_billing": 0, "total_yoy": 0, "top5_accounts": [], "top5_prdgs": [], "account_list": [], "cnt_cred_block": 0, "cnt_del_block": 0, "cnt_unconf_delay": 0, "cnt_do_delay": 0, "cnt_incomplete": 0}
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
        
###### SCHEDULER ENTRY POINT
def plan_execution():
    """
    plan_execution(execution_id)
    1. User Pool 확인
    2. Target 시각 확인
    3. 분산처리 Plan
    4. make request_dict -> execute_dair
    5. load today + delete prev
    """
    oracle = ORACLE()
    # 이후 운영에 AI_USE_YN 반영되면 주석 해제
    target_users = oracle.fetch_ai_users()
    # target_users = [('arum33.sim',), ('hooguen.baek',), ('admin',), ('seokhoon.son_7103',)]
    print(target_users)
    for (user_id,) in target_users:
        test = ScenarioResult(
            execution_id=str(uuid.uuid4()),
            sender_name=user_id,
            user_id=user_id,
            chatroom_id=str(uuid.uuid4())  # unique chatroom per user
        )
        execute_dair(test)
    return
 
def save_scenario_result_to_db_rdb(scenario_result):
    """
    Save parts of scenario_result to user_scenario_result table using RDB class.
    Args:
        rdb: RDB instance (Postgres helper)
        scenario_result: object with scenario result attributes
        user_id: str, user identifier
    """
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
 
 
###### MAIN SERVICE ENTRY POINT
# DAIR: Daily Action Item Recommendation Service
def execute_dair(scenario_result): # : ScenarioResult
    """
    1. INIT Ontology
    2. TRAVERSE Scenario + Action
    3. Load Cache to pkl (user_id, execution_id)
    """
    # USER INFO 불러오기
    user_manager = UserManager()
    user_info = user_manager.get_or_create_user_info(f"{scenario_result.user_id}", f"{scenario_result.user_id}", f"{scenario_result.execution_id}", "teams", "hooguen.baek")
   
    # Calculate Local Time
    server_time = time.time()
    local_time = calc_local_time(server_time=server_time, user_sales_org=user_info.user_sales_org)
    scenario_result.server_time = server_time #datetime.datetime
    scenario_result.local_time = local_time #datetime.datetime
   
   
    from src.modules.agents.analysis.resources.dair_ontology import DAIR
   
    # DAIR ontology
    dair = DAIR()
    RESOURCE_DIR = os.path.join(PROJECT_ROOT, "src","modules", "agents", "analysis", "resources")
    ONTOLOGY_FILE = os.path.join(RESOURCE_DIR, "ontology_raw.txt")
    dair.load_from_csv(ONTOLOGY_FILE)
   
    # print("Tree Structure:")
    # dair.print_tree()
   
    print(f"\nStatistics: {dair.get_statistics()}")
   
    # print("\nTraversing Actions:")
    # actions = dair.traverse_actions()
    # for i, action in enumerate(actions, 1):
    #     path = dair.get_path_to_action(action.node_id)
    #     path_str = " -> ".join([node.node_id for node in path])
    #     print(f"{i}. {path_str}")
   
    print("\nExecuting Action Sequence:")
    def custom_callback(action_node, path, scenario_result, user_info):
        from src.modules.agents.analysis.utils.action_registry import action_registry
       
        path_str = " -> ".join([node.node_id for node in path])
        print(f"Executing action: {action_node.node_id} (Path: {path_str})")
 
        return action_registry[f"{action_node.node_id}"](scenario_result, user_info)
 
    print("execute_action_sequence")  
    scenario_result = dair.execute_action_sequence(custom_callback, scenario_result, user_info)
   
    # Load scenario_result to pkl (user_id + execution_id)
    load_scenario_result_to_pkl(scenario_result)
   
    # Load system logs
   
    # load_system_log(scenario_result)
    save_scenario_result_to_db_rdb(scenario_result)
   
    print("🎁🎁")
    return
 
 
###### MISC
def calc_local_time(server_time: float, user_sales_org: str) -> datetime.datetime:
    """
    Convert server time (epoch seconds) to local time given a GMT offset.
 
    Args:
        server_time (float): Epoch time from time.time(), assumed UTC-based.
        gmt_offset_str (str): GMT offset as a string (e.g., "-9", "+5.5").
 
    Returns:
        datetime.datetime: Local datetime adjusted by the GMT offset.
    """
    # fetch user time zone
    gmt_offset_str = ORACLE().fetch_tz_info(user_sales_org)
   
    # Convert server epoch time to UTC datetime
    utc_time = datetime.datetime.utcfromtimestamp(server_time)
 
    # Convert GMT offset string to float hours
    offset_hours = float(gmt_offset_str)
 
    # Apply offset as timedelta
    local_time = utc_time + datetime.timedelta(hours=offset_hours)
    return local_time
 
###### Loading Func
def load_scenario_result_to_pkl(scenario_result):
    """ Save Scenario Result to Pickle file """
    cache_path = r"/APPL/space_backend/space_fast/src/modules/agents/analysis/scenario_cache"
    print("scenario_result")
    print(scenario_result)
    user_id = scenario_result.user_id
    user_id_rewritten = user_id.replace(".", "")
    execution_id = scenario_result.execution_id
   
    fname = f"{user_id_rewritten}_{execution_id}.pkl"
    full_path = os.path.join(cache_path, fname)
    with open(full_path, "wb") as f:
        pickle.dump(scenario_result, f)
       
    return
 
def load_system_log(scenario_result):
    """ Load System Log to RDB """
    # Execution Log
    from src.modules.agents.analysis.orm.postgres import RDB
    rdb = RDB()
   
    execution_log = {
        'user_id' : scenario_result.user_id,
        'execution_id' : str(scenario_result.execution_id),
        'execution_ts' : scenario_result.server_time,
        'success_yn' : True,
        'error_msg' : ""
    }
    rdb.load_execution_log(required_inputs=execution_log)
   
    home_schedule = {
        'user_id' : scenario_result.user_id,
        'execution_id' : str(scenario_result.execution_id),
        'last_update_ts' : scenario_result.server_time,
        'next_target_yn' : True
    }
    rdb.load_home_schedule(required_inputs=home_schedule)
   
    return
 
###### DEBUG
if __name__ == "__main__":
    uid = uuid.uuid4()
    test = ScenarioResult(
        execution_id = f"{uid}",
        sender_name = "hooguen",
        user_id = "hooguen.baek",
        chatroom_id = "99998888"
    )
   
    execute_dair(test)
 
    # execute_dair(None)
    # plan_execution()
    pass
 