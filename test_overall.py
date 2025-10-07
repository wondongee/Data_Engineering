#!/usr/bin/env python3
"""
스케줄링 시스템 테스트 스크립트
Redis Stream 기반 2단계 파이프라인 테스트
"""
 
import os, sys, time, logging
import redis
import threading
import subprocess
import signal
from datetime import datetime
 
# 프로젝트 경로 설정
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))
sys.path.append(PROJECT_ROOT)
 
from app import config
from app.window import current_window_id
from src.modules.agents.analysis.orm.oracle import ORACLE
from app.scheduler import (enqueue_sales_org_jobs, enqueue_adaptive_card_jobs,
                          do_merge_after_sales_org)
from app.stream_monitor import StreamMonitor, print_status_table
 
# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("test_scheduler")
 
# Redis 연결
r = redis.from_url(config.REDIS_URL, decode_responses=True)
monitor = StreamMonitor(r)
 
def check_redis_connection():
    """Redis 연결 확인"""
    try:
        r.ping()
        log.info("✅ Redis 연결 성공")
        return True
    except Exception as e:
        log.error(f"❌ Redis 연결 실패: {e}")
        return False
 
def clear_queues():
    """모든 큐 초기화"""
    try:
        # 스트림 삭제
        r.delete(config.SALES_ORG_QUEUE)
        r.delete(config.ADAPTIVE_CARD_QUEUE)
        log.info("🧹 큐 초기화 완료")
    except Exception as e:
        log.warning(f"큐 초기화 중 오류: {e}")
 
def check_queue_status():
   """큐 상태 + pending 상태 확인 (개선된 버전)"""
   try:
       # 새로운 모니터링 시스템 사용
       status = monitor.get_pipeline_status(
           config.SALES_ORG_QUEUE, config.SALES_ORG_GROUP,
           config.ADAPTIVE_CARD_QUEUE, config.ADAPTIVE_CARD_GROUP
       )
       
       sales = status["sales_org"]
       adaptive = status["adaptive_card"]
       
       log.info(
           f"📊 Pipeline Status: {status['pipeline_status']} | "
           f"SalesOrg: {sales['status']} (stream={sales['stream_length']}, pending={sales['pending_count']}) | "
           f"AdaptiveCard: {adaptive['status']} (stream={adaptive['stream_length']}, pending={adaptive['pending_count']})"
       )
       
       return (sales['stream_length'], sales['pending_count']), (adaptive['stream_length'], adaptive['pending_count'])
       
   except Exception as e:
       log.error(f"❌ check_queue_status 오류: {e}")
       return (0, 0), (0, 0)

def test_enqueue():
    """파이프라인 실행 테스트"""
    log.info("🚀 파이프라인 테스트 시작")
   
    ora = ORACLE()
    sales_org_ids = ora.fetch_sales_org_code()
    user_ids = ora.fetch_ai_users()
       
    log.info(f"📋 테스트 데이터: Sales Org={len(sales_org_ids)}개, Users={len(user_ids)}개")
   
    # 큐 적재
    enqueue_sales_org_jobs(sales_org_ids)
    enqueue_adaptive_card_jobs(user_ids)
   
    log.info("✅ 큐 적재 완료")
    check_queue_status()
 
def monitor_queues(duration=120):
    """큐 모니터링"""
    log.info(f"👀 모니터링 시작 ({duration}초)")
    start_time = time.time()
   
    while time.time() - start_time < duration:
        try:
            status = monitor.get_pipeline_status(
                config.SALES_ORG_QUEUE, config.SALES_ORG_GROUP,
                config.ADAPTIVE_CARD_QUEUE, config.ADAPTIVE_CARD_GROUP
            )
            
            sales = status["sales_org"]
            adaptive = status["adaptive_card"]
            
            log.info(
                f"📈 {status['pipeline_status']} | "
                f"Sales: {sales['stream_length']}/{sales['pending_count']} | "
                f"Adaptive: {adaptive['stream_length']}/{adaptive['pending_count']}"
            )
            
            if status['pipeline_status'] == 'completed':
                log.info("🎉 완료!")
                break
                
        except Exception as e:
            log.error(f"모니터링 오류: {e}")
       
        time.sleep(5)
   
    log.info("✅ 모니터링 완료")

def run_workers():
   """워커 실행 테스트"""
   log.info("🔧 워커 실행 테스트")
   subprocess.run([sys.executable, "-m", "app.worker_adaptive_card"], check=True, cwd=PROJECT_ROOT)
 
def main():
    """메인 테스트 메뉴"""
    print("\n" + "="*60)
    print("🧪 스케줄링 시스템 테스트 도구")
    print("="*60)
   
    if not check_redis_connection():
        return
   
    while True:
        print("\n테스트 옵션을 선택하세요:")
        print("1. 큐 상태 확인")
        print("2. 큐 초기화")
        print("3. 파이프라인 테스트")
        print("4. 모니터링 (2분)")
        print("5. 워커 테스트")
        print("0. 종료")
       
        choice = input("\n선택: ").strip()
       
        try:
            if choice == "1":
                check_queue_status()
            elif choice == "2":
                clear_queues()
            elif choice == "3":
                test_enqueue()
            elif choice == "4":
                monitor_queues()          
            elif choice == "5":
                run_workers()
            elif choice == "0":
                log.info("👋 테스트 종료")
                break
            else:
                print("❌ 잘못된 선택입니다.")
        except KeyboardInterrupt:
            log.info("\n⏹️  테스트 중단됨")
            break
        except Exception as e:
            log.error(f"❌ 오류 발생: {e}")
 
if __name__ == "__main__":
    main()