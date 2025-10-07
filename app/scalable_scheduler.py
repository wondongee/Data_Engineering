# app/scalable_scheduler.py

import os, sys, time, logging, redis
import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from concurrent.futures import ThreadPoolExecutor, as_completed

# 프로젝트 경로 / 설정
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)
from app import config
from app.window import current_window_id
from src.modules.agents.analysis.orm.oracle import ORACLE

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("scalable_scheduler")
for name in ["apscheduler", "apscheduler.scheduler", "apscheduler.executors.default", "apscheduler.jobstores.default"]:
    logging.getLogger(name).setLevel(logging.WARNING)

# Redis 연결
r = redis.from_url(config.REDIS_URL, decode_responses=True)

class ScalableScheduler:
    """
    수평 확장 가능한 스케줄러
    - 여러 워커 인스턴스 동시 실행
    - Consumer Group을 통한 로드 밸런싱
    - 동적 워커 수 조정
    """
    
    def __init__(self):
        self.max_workers_sales = int(os.getenv("MAX_WORKERS_SALES", "3"))
        self.max_workers_adaptive = int(os.getenv("MAX_WORKERS_ADAPTIVE", "3"))
        self.executor = ThreadPoolExecutor(max_workers=max(self.max_workers_sales, self.max_workers_adaptive))
        
    def enqueue_sales_org_jobs(self, sales_org_ids):
        """Sales Org 작업들을 큐에 등록"""
        window_id, _, _ = current_window_id()
        for sid in sales_org_ids:
            r.xadd(
                config.SALES_ORG_QUEUE,
                {
                    "sales_org_id": sid,
                    "window_id": window_id,
                    "stage": "sales_org_fetch",
                    "created_at": time.time(),
                    "retry_count": "0"  # 재시도 카운터 초기화
                },
                maxlen=5000, approximate=True
            )
        log.info(f"[enqueue] sales_org jobs={len(sales_org_ids)}  xlen={r.xlen(config.SALES_ORG_QUEUE)}")
        
    def enqueue_adaptive_card_jobs(self, user_ids):
        """Adaptive Card 작업들을 큐에 등록"""
        window_id, _, _ = current_window_id()
        for uid in user_ids:
            r.xadd(
                config.ADAPTIVE_CARD_QUEUE,
                {
                    "user_id": uid[0],
                    "window_id": window_id,
                    "stage": "adaptive_card_generation",
                    "created_at": time.time(),
                    "retry_count": "0"  # 재시도 카운터 초기화
                },
                maxlen=5000, approximate=True
            )
        log.info(f"[enqueue] adaptive_card jobs={len(user_ids)}  xlen={r.xlen(config.ADAPTIVE_CARD_QUEUE)}")

    def run_worker_batch(self, worker_module, worker_count, timeout_seconds=1800):
        """
        여러 워커 인스턴스를 병렬로 실행
        
        Args:
            worker_module: 실행할 워커 모듈명
            worker_count: 실행할 워커 수
            timeout_seconds: 타임아웃 (초)
        """
        log.info(f"[spawn] Starting {worker_count} instances of {worker_module}")
        
        # 워커 프로세스들을 병렬로 시작
        futures = []
        for i in range(worker_count):
            future = self.executor.submit(
                self._run_single_worker, 
                worker_module, 
                f"worker-{i+1}"
            )
            futures.append(future)
        
        # 모든 워커 완료 대기 (타임아웃 적용)
        completed_count = 0
        for future in as_completed(futures, timeout=timeout_seconds):
            try:
                result = future.result()
                completed_count += 1
                log.info(f"[spawn] Worker completed: {result}")
            except Exception as e:
                log.error(f"[spawn] Worker failed: {e}")
        
        log.info(f"[spawn] {completed_count}/{worker_count} workers completed for {worker_module}")

    def _run_single_worker(self, worker_module, worker_name):
        """단일 워커 실행"""
        try:
            # 환경변수로 워커 이름 설정
            env = dict(os.environ, PYTHONUNBUFFERED="1", WORKER_NAME=worker_name)
            
            result = subprocess.run(
                [sys.executable, "-m", worker_module],
                cwd=PROJECT_ROOT,
                check=True,
                capture_output=True,
                text=True,
                timeout=1800,  # 30분 타임아웃
                env=env
            )
            
            return f"{worker_name}: SUCCESS"
            
        except subprocess.TimeoutExpired:
            log.error(f"[spawn] {worker_name} timed out after 30 minutes")
            return f"{worker_name}: TIMEOUT"
        except subprocess.CalledProcessError as e:
            log.error(f"[spawn] {worker_name} failed with return code {e.returncode}")
            return f"{worker_name}: FAILED ({e.returncode})"
        except Exception as e:
            log.error(f"[spawn] {worker_name} exception: {e}")
            return f"{worker_name}: EXCEPTION"

    def wait_for_queue_completion(self, queue_name, group_name, max_wait_seconds=1800):
        """큐 완료 대기 (간소화)"""
        log.info(f"[wait] Waiting for {queue_name} completion...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            stream_length = r.xlen(queue_name)
            pending_info = r.xpending(queue_name, group_name)
            pending_count = pending_info.get("count", 0) if pending_info else 0
            
            if stream_length == 0 and pending_count == 0:
                log.info(f"[wait] {queue_name} completed!")
                return True
                
            time.sleep(5)  # 5초마다 확인
        
        log.warning(f"[wait] {queue_name} timeout after {max_wait_seconds}s")
        return False

    def do_merge_after_sales_org(self):
        """Sales Org 작업 완료 후 CSV 머지"""
        from app.scheduler import do_merge_after_sales_org as merge_func
        merge_func()

    def run_scalable_pipeline(self):
        """수평 확장 가능한 파이프라인 실행"""
        log.info("🚀 Scalable pipeline start")
        
        try:
            # 1. 데이터 수집
            ora = ORACLE()
            sales_org_ids = ora.fetch_sales_org_code()
            user_ids = ora.fetch_ai_users()
            
            # 2. Sales Org 단계
            log.info(f"[pipeline] Phase 1: Processing {len(sales_org_ids)} sales orgs with {self.max_workers_sales} workers")
            self.enqueue_sales_org_jobs(sales_org_ids)
            self.run_worker_batch("app.worker_sales_org", self.max_workers_sales)
            
            # 큐 완료 대기
            self.wait_for_queue_completion(config.SALES_ORG_QUEUE, config.SALES_ORG_GROUP)
            
            # 3. 머지
            self.do_merge_after_sales_org()
            
            # 4. Adaptive Card 단계
            log.info(f"[pipeline] Phase 2: Processing {len(user_ids)} users with {self.max_workers_adaptive} workers")
            self.enqueue_adaptive_card_jobs(user_ids)
            self.run_worker_batch("app.worker_adaptive_card", self.max_workers_adaptive)
            
            # 큐 완료 대기
            self.wait_for_queue_completion(config.ADAPTIVE_CARD_QUEUE, config.ADAPTIVE_CARD_GROUP)
            
            log.info("✅ Scalable pipeline finished")
            
        except Exception as e:
            log.error(f"❌ Pipeline failed: {e}")
            raise

    def cleanup_queues(self):
        """큐 정리"""
        log.info("🧹 Cleaning up queues...")
        try:
            r.delete(config.SALES_ORG_QUEUE)
            r.delete(config.ADAPTIVE_CARD_QUEUE)
            log.info("✅ Queue cleanup completed")
        except Exception as e:
            log.error(f"❌ Queue cleanup failed: {e}")

    def get_queue_stats(self):
        """큐 상태 조회 (간소화)"""
        try:
            return {
                "sales_org_queue": {
                    "length": r.xlen(config.SALES_ORG_QUEUE),
                    "pending": r.xpending(config.SALES_ORG_QUEUE, config.SALES_ORG_GROUP).get("count", 0)
                },
                "adaptive_card_queue": {
                    "length": r.xlen(config.ADAPTIVE_CARD_QUEUE),
                    "pending": r.xpending(config.ADAPTIVE_CARD_QUEUE, config.ADAPTIVE_CARD_GROUP).get("count", 0)
                }
            }
        except:
            return {"sales_org_queue": {"length": 0, "pending": 0}, "adaptive_card_queue": {"length": 0, "pending": 0}}

    def shutdown(self):
        """스케줄러 종료"""
        log.info("🛑 Shutting down scalable scheduler...")
        self.executor.shutdown(wait=True)
        log.info("✅ Scheduler shutdown completed")

def main():
    """메인 실행 함수"""
    log.info(f"[bootstrap] workspace={PROJECT_ROOT}")
    
    scheduler = ScalableScheduler()
    
    # 시작 전 큐 상태 로깅
    stats = scheduler.get_queue_stats()
    log.info(f"[bootstrap] Initial queue stats: {stats}")
    
    # 스케줄러 설정
    sched = BlockingScheduler(timezone=config.TZ)
    m, h, d, mo, dow = config.CRON_SPEC.split()
    cron = CronTrigger(minute=m, hour=h, day=d, month=mo, day_of_week=dow)
    
    sched.add_job(
        scheduler.run_scalable_pipeline,
        trigger=cron,
        id="scalable-pipeline",
        coalesce=True,
        misfire_grace_time=300,
        replace_existing=True,
        max_instances=1,
    )
    
    log.info(f"[bootstrap] Scheduled with CRON_SPEC='{config.CRON_SPEC}'")
    log.info(f"[bootstrap] Max workers - Sales: {scheduler.max_workers_sales}, Adaptive: {scheduler.max_workers_adaptive}")
    
    # 큐 정리
    scheduler.cleanup_queues()
    
    try:
        sched.start()
    except KeyboardInterrupt:
        log.info("Received interrupt signal")
    finally:
        scheduler.shutdown()

if __name__ == "__main__":
    main()
