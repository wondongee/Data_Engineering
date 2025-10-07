# app/scheduler.py
 
import os, sys, time, logging, re
import pandas as pd
import redis
import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from pathlib import Path
 
# ─────────────────────────────────────────────────────────────────────────────
# 프로젝트 경로 / 설정
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)
from app import config
from app.window import current_window_id
from src.modules.agents.analysis.orm.oracle import ORACLE
 
# ──────────────────────────────────────────────────────────────────────
# 로깅 (우리만 INFO, APScheduler는 WARNING으로 정리)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("scheduler")
for name in ["apscheduler", "apscheduler.scheduler", "apscheduler.executors.default", "apscheduler.jobstores.default"]:
   logging.getLogger(name).setLevel(logging.WARNING)
   
# Redis 연결
r = redis.from_url(config.REDIS_URL, decode_responses=True)
 
# enqueue 단계
def enqueue_sales_org_jobs(sales_org_ids):
   window_id, _, _ = current_window_id()
   for sid in sales_org_ids:
       r.xadd(
           config.SALES_ORG_QUEUE,
           {
               "sales_org_id": sid,
               "window_id": window_id,
               "stage": "sales_org_fetch",
               "created_at": time.time(),
           },
           maxlen=5000, approximate=True # 폭주 방지
       )
   log.info(f"[enqueue] sales_org jobs={len(sales_org_ids)}  xlen={r.xlen(config.SALES_ORG_QUEUE)}")
   
def enqueue_adaptive_card_jobs(user_ids):
   window_id, _, _ = current_window_id()
   for uid in user_ids:
       r.xadd(
           config.ADAPTIVE_CARD_QUEUE,
           {
               "user_id": uid[0],
               "window_id": window_id,
               "stage": "adaptive_card_generation",
               "created_at": time.time(),
           },
           maxlen=5000, approximate=True
       )
   log.info(f"[enqueue] adaptive_card jobs={len(user_ids)}  xlen={r.xlen(config.ADAPTIVE_CARD_QUEUE)}")
 
def clear_queues(r):
    """기존 큐 데이터 정리"""
    try:
        r.delete(config.SALES_ORG_QUEUE)
        r.delete(config.ADAPTIVE_CARD_QUEUE)
        log.info("🧹 큐 정리 완료")
    except Exception as e:
        log.error(f"❌ 큐 정리 실패: {e}")
 
def do_merge_after_sales_org():
   """Material/GSCM CSV 파일들을 머지"""
   window_id, _, _ = current_window_id()
   out_dir = Path(config.CSV_OUTPUT_DIR)
   log.info(f"[merge] Starting merge for window_id='{window_id}'")
   
   for prefix in ("Material", "GSCM"):
       files = sorted(out_dir.glob(f"{prefix}_*.csv"))
       if not files:
           continue
           
       dfs = []
       for f in files:
           try:
               dfs.append(pd.read_csv(f))
           except Exception as e:
               log.warning(f"[merge] Skip {f.name}: {e}")
               
       if not dfs:
           continue
           
       merged = pd.concat(dfs, ignore_index=True)
       tmp = out_dir / f"{prefix}_MERGED_{window_id}.csv.tmp"
       final = out_dir / f"{prefix}.csv"
       merged.to_csv(tmp, index=False)
       tmp.replace(final)
       log.info(f"[merge] {prefix} merged: {len(merged)} rows from {len(dfs)} files")
 
def _run_worker_once(py_module: str):
   log.info(f"[spawn] python -m {py_module}")
   subprocess.run(
       [sys.executable, "-m", py_module],
       cwd=PROJECT_ROOT,
       check=True,              # 실패 시 예외
       env=dict(os.environ, PYTHONUNBUFFERED="1"),  # 버퍼링 방지(권장)
   )
   log.info(f"[spawn] {py_module} finished")
   
# ─────────────────────────────────────────────────────────────────────────────
# 파이프라인 1회 실행(원샷): 1차 → 대기 → 머지 → 2차 → 대기
 
def run_pipeline_once():
   log.info("🚀 pipeline start")
   ora = ORACLE()
   sales_org_ids = ora.fetch_sales_org_code()
   user_ids      = ora.fetch_ai_users()
   
   # 1) 큐1 적재
   enqueue_sales_org_jobs(sales_org_ids)
   # 2) 워커1 한 번 실행 (배치 처리 후 자동 종료)
   _run_worker_once("app.worker_sales_org")
   # 3) 머지
   do_merge_after_sales_org()
   # 4) 큐2 적재
   enqueue_adaptive_card_jobs(user_ids)
   # 5) 워커2 한 번 실행 (배치 처리 후 자동 종료)
   _run_worker_once("app.worker_adaptive_card")
   
   log.info("✅ pipeline finished")
# ─────────────────────────────────────────────────────────────────────────────
# 스케줄러 엔트리
def main():
   log.info(f"[bootstrap] workspace={PROJECT_ROOT}")
   log.info(f"[bootstrap] queues before start: sales={r.xlen(config.SALES_ORG_QUEUE)}, adaptive={r.xlen(config.ADAPTIVE_CARD_QUEUE)}")
   sched = BlockingScheduler(timezone=config.TZ)
   # CRON_SPEC 예) "* * * * *" (매분), "0/30 * * * *" (30초마다), "0 */2 * * *" (2시간 간격)
   m, h, d, mo, dow = config.CRON_SPEC.split()
   cron = CronTrigger(minute=m, hour=h, day=d, month=mo, day_of_week=dow)
   sched.add_job(
       run_pipeline_once,
       trigger=cron,
       id="pipeline-once",
       coalesce=True,
       misfire_grace_time=300,
       replace_existing=True,
       max_instances=1,  # 이전 실행이 끝나지 않았으면 겹치지 않도록
   )
   log.info(f"[bootstrap] scheduled with CRON_SPEC='{config.CRON_SPEC}'")
   
   clear_queues(r)
   sched.start()
 
if __name__ == "__main__":
   main()