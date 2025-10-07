# app/worker_sales_org.py
import os, sys, time, logging, redis, re, socket
from pathlib import Path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)
from app import config
from app.retry_handler import RetryHandler, retry_on_failure
from src.modules.agents.analysis.scenario_preprocessor import (
    make_sales_org_pool, make_sales_org_pool_one, make_sales_org_total_pool_one
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker_sales_org")
 
r = redis.from_url(config.REDIS_URL, decode_responses=True)
STREAM   = config.SALES_ORG_QUEUE
GROUP    = config.SALES_ORG_GROUP
# 워커 이름을 환경변수에서 가져오거나 기본값 사용
WORKER_NAME = os.getenv("WORKER_NAME", f"{config.SALES_ORG_WORKER_NAME}-{socket.gethostname()}-{os.getpid()}")
CONSUMER = WORKER_NAME  # 고유한 consumer 이름
OUT_DIR = Path(config.CSV_OUTPUT_DIR)  # ✅ 활성화

# 재시도 핸들러 초기화
retry_handler = RetryHandler(r, STREAM, GROUP, CONSUMER)
 
def _safe(s: str) -> str:
   return re.sub(r"[^A-Za-z0-9_\-T:.]", "-", str(s))
 
def ensure_group():
   try:
       r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
       logging.info(f"[sales_org_worker] XGROUP CREATE {STREAM} {GROUP}")
   except redis.exceptions.ResponseError as e:
       if "BUSYGROUP" in str(e):
           pass
       else:
           raise
       
def _save_df(df, kind: str, sales_org_id: str, window_id: str) -> Path | None:
   """
   kind: 'Material' 또는 'GSCM'
   파일명에만 window_id 반영. 데이터 컬럼은 그대로.
   """
   if df is None or df.empty:
       return None
   OUT_DIR.mkdir(parents=True, exist_ok=True)
   #fname = f"{kind}_{_safe(sales_org_id)}_{_safe(window_id)}.csv"
   fname = f"{kind}_{_safe(sales_org_id)}.csv"
   tmp = OUT_DIR / (fname + ".tmp")
   final = OUT_DIR / fname
   df.to_csv(tmp, index=False)
   tmp.replace(final)  # 원자적 교체
   return final
 
@retry_on_failure(max_retries=3, base_delay=1.0)
def _process_sales_org_data(sales_org_id, window_id):
   """Sales Org 데이터 처리 (재시도 가능한 핵심 로직)"""
   log.info(f"[{WORKER_NAME}] Processing sales_org={sales_org_id}")
   
   # DF 생성 및 저장
   df_mat  = make_sales_org_pool_one(sales_org_id, window_id)
   df_gscm = make_sales_org_total_pool_one(sales_org_id, window_id)
   
   mat_path  = _save_df(df_mat,  "Material", sales_org_id, window_id)
   gscm_path = _save_df(df_gscm, "GSCM",     sales_org_id, window_id)
   
   log.info(f"[{WORKER_NAME}] Completed sales_org={sales_org_id}")

def handle_sales_org_message(message_id, fields):
   """
   메시지 1개(= Sales Org 1개) 처리:
     1) MATERIAL 생성 → CSV 저장
     2) TOTAL(GSCM) 생성 → CSV 저장
     3) 재시도 로직 적용
   """
   sales_org_id = fields.get("sales_org_id")
   window_id    = fields.get("window_id")
   stage        = fields.get("stage")
   
   if stage != "sales_org_fetch":
       logging.warning(f"[sales_org_worker] Unexpected stage: {stage}")
       return
   
   # 재시도 로직과 함께 처리
   success = retry_handler.process_with_retry(
       message_id=message_id,
       fields=fields,
       handler_func=lambda mid, f: _process_sales_org_data(
           f.get("sales_org_id"), 
           f.get("window_id")
       ),
       max_retries=3,
       base_delay=1.0,
       max_delay=60.0
   )
   
   if not success:
       logging.error(f"[sales_org_worker] Failed to process message {message_id} after all retries")      
 
def run_batch_once():
   ensure_group()  # XGROUP CREATE(stream, group, id='0', mkstream=True)
   log.info(f"[{WORKER_NAME}] batch start: STREAM={STREAM} GROUP={GROUP} CONSUMER={CONSUMER}")
   while True:
       # 1) 아직 전달 안 된 새 메시지('>')부터 읽기 (배치라 block=0)
       resp = r.xreadgroup(
           GROUP, CONSUMER,
           streams={STREAM: ">"},
           count=config.BATCH_COUNT,
           block=1000
       )
       if resp:
           # 2) 있으면 처리
           for _, messages in resp:
               for message_id, fields in messages:
                   handle_sales_org_message(message_id, fields)  # 또는 handle_adaptive_card_message
           continue
       # 3) 없으면 종료 조건 점검
       try:
           pend_info = r.xpending(STREAM, GROUP) or {}
           pending = (pend_info.get("count") or pend_info.get("pending") or 0)
       except Exception:
           pending = 0
       xlen = r.xlen(STREAM)
       if xlen == 0 and pending == 0:
           log.info("[worker] queue drained → exit")
           break
       time.sleep(0.5)
               
   
def main():
   run_batch_once()
   
if __name__ == "__main__":
   main()