# app/worker_adaptive_card.py
import os, sys
import json, time, shutil, logging, socket
import redis
from pathlib import Path

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)

from app import config
from app.window import current_window_id
from app.retry_handler import RetryHandler, retry_on_failure

# 시나리오 엔진(변경 금지 파일)의 모델/함수 활용
from src.modules.agents.analysis.scenario_preprocessor import ScenarioResult, execute_dair, save_scenario_result_to_db_rdb
from src.modules.agents.analysis.utils.action_registry import CONFIG as AR_CFG
 
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker_adaptive_card")
 
r = redis.from_url(config.REDIS_URL, decode_responses=True)
STREAM   = config.ADAPTIVE_CARD_QUEUE
GROUP    = config.ADAPTIVE_CARD_GROUP
# 워커 이름을 환경변수에서 가져오거나 기본값 사용
WORKER_NAME = os.getenv("WORKER_NAME", f"{config.ADAPTIVE_CARD_WORKER_NAME}-{socket.gethostname()}-{os.getpid()}")
CONSUMER = WORKER_NAME  # 고유한 consumer 이름

# 재시도 핸들러 초기화
retry_handler = RetryHandler(r, STREAM, GROUP, CONSUMER)
 
def ensure_group():
   try:
       r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
       log.info(f"[adaptive_card_worker] XGROUP CREATE {STREAM} {GROUP}")
   except redis.exceptions.ResponseError as e:
       if "BUSYGROUP" in str(e):
           pass
       else:
           raise
       
def _result_to_json_payload(sr: ScenarioResult) -> dict:
   """ScenarioResult에서 페이지 묶음 JSON으로 변환."""
   
   return {
       "user_id": sr.user_id,
       "window_id": sr.chatroom_id,  # window_id 개념과 별개면 필요시 필드 교체
       "pages": {
           "LAPA00": sr.LAPA00 or {},
           "SOSE00": sr.SOSE00 or {},
           "SODE00": sr.SODE00 or {},
           "PGSE00": sr.PGSE00 or {},
           "PGDE00": sr.PGDE00 or {},
           "MODE00": sr.MODE00 or {},
           "OPTR00": sr.OPTR00 or {},
       },
       "meta": {
           "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "source": "scenario_execution.execute_dair",
       }
   }
 
def _save_pages_json(payload: dict, window_id: str, user_id: str) -> Path:
   """사용자/윈도우 단위 JSON 저장."""
   out_dir = Path(config.CSV_OUTPUT_DIR)
   out_dir.mkdir(parents=True, exist_ok=True)
   fname = out_dir / f"AdaptiveCard_{user_id}.json"
   tmp = Path(str(fname) + ".tmp")
   tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
   os.replace(tmp, fname)
   return fname
 
@retry_on_failure(max_retries=3, base_delay=1.0)
def _process_adaptive_card_data(user_id, window_id):
   """Adaptive Card 데이터 처리 (재시도 가능한 핵심 로직)"""
   log.info(f"[{WORKER_NAME}] Processing user={user_id}")
   
   # ScenarioResult 준비
   sr = ScenarioResult(
       execution_id=window_id,
       sender_name=user_id,
       user_id=user_id,
       chatroom_id=window_id,
   )
   
   # 시나리오 실행 및 DB 저장
   sr = execute_dair(sr)
   save_scenario_result_to_db_rdb(sr)
   
   log.info(f"[{WORKER_NAME}] Completed user={user_id}")

def handle_adaptive_card_message(message_id, fields):
   """
   메시지 1개(= 사용자 1명) 처리:
     1) ScenarioResult 구성 후 execute_dair() 실행 -> 페이지 결과 생성
     2) 결과 DB 저장
     3) 재시도 로직 적용
   """
   user_id  = str(fields.get("user_id"))   # 문자열로 통일
   window_id = fields.get("window_id")
   stage    = fields.get("stage")
   
   if stage != "adaptive_card_generation":
       log.warning(f"[adaptive_card_worker] Unexpected stage: {stage}")
       return
   
   # 재시도 로직과 함께 처리
   success = retry_handler.process_with_retry(
       message_id=message_id,
       fields=fields,
       handler_func=lambda mid, f: _process_adaptive_card_data(
           f.get("user_id"), 
           f.get("window_id")
       ),
       max_retries=3,
       base_delay=1.0,
       max_delay=60.0
   )
   
   if not success:
       log.error(f"[{WORKER_NAME}] Failed to process message {message_id} after all retries")
 
def run_batch_once():
   ensure_group()
   log.info(f"[{WORKER_NAME}] batch start: STREAM={STREAM} GROUP={GROUP} CONSUMER={CONSUMER}")
   while True:
       # 새 메시지 읽기
       resp = r.xreadgroup(
           GROUP, CONSUMER,
           streams={STREAM: ">"},
           count=config.BATCH_COUNT,
           block=1000
       )
       if resp:
           # 메시지 있으면 처리
           for _, messages in resp:
               for message_id, fields in messages:
                   handle_adaptive_card_message(message_id, fields)
           continue  # 처리했으니 다음 루프
       # 메시지 없으면 종료 조건 확인
       xlen = r.xlen(STREAM)
       pend_info = r.xpending(STREAM, GROUP) or {}
       pend = pend_info.get("count") or pend_info.get("pending") or 0
       if xlen == 0 and pend == 0:
           log.info("[adaptive_card_worker] queue drained → exit")
           break
       time.sleep(0.5)
               
def main():
   run_batch_once()
   
if __name__ == "__main__":
   main()