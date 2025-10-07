# app/stream_monitor.py

import redis
import logging
from typing import Dict

log = logging.getLogger(__name__)

class StreamMonitor:
    """Redis Stream 상태 모니터링 (간소화 버전)"""
    
    def __init__(self, redis_client: redis.Redis):
        self.r = redis_client
    
    def get_stream_status(self, stream_name: str, group_name: str) -> Dict:
        """Stream 상태 조회"""
        try:
            stream_length = self.r.xlen(stream_name)
            pending_info = self.r.xpending(stream_name, group_name)
            pending_count = pending_info.get("count", 0) if pending_info else 0
            
            # 상태 판단
            if stream_length == 0 and pending_count == 0:
                status = "completed"
            elif pending_count > 0:
                status = "processing"
            elif stream_length > 0:
                status = "queued"
            else:
                status = "idle"
            
            return {
                "stream_length": stream_length,
                "pending_count": pending_count,
                "status": status
            }
            
        except Exception as e:
            log.error(f"Failed to get stream status for {stream_name}: {e}")
            return {"stream_length": 0, "pending_count": 0, "status": "error"}
    
    def get_pipeline_status(self, sales_stream: str, sales_group: str, 
                           adaptive_stream: str, adaptive_group: str) -> Dict:
        """전체 파이프라인 상태 조회"""
        sales_status = self.get_stream_status(sales_stream, sales_group)
        adaptive_status = self.get_stream_status(adaptive_stream, adaptive_group)
        
        # 파이프라인 상태 판단
        if sales_status["status"] == "completed" and adaptive_status["status"] == "completed":
            pipeline_status = "completed"
        elif sales_status["status"] in ["processing", "queued"]:
            pipeline_status = "phase1_sales_org"
        elif adaptive_status["status"] in ["processing", "queued"]:
            pipeline_status = "phase2_adaptive_card"
        else:
            pipeline_status = "idle"
        
        return {
            "pipeline_status": pipeline_status,
            "sales_org": sales_status,
            "adaptive_card": adaptive_status
        }
    
def print_status_table(status_dict: Dict):
    """상태 정보를 간단한 테이블 형태로 출력"""
    print("\n" + "="*60)
    print(f"📊 Pipeline Status: {status_dict['pipeline_status'].upper()}")
    print("="*60)
    
    sales = status_dict["sales_org"]
    adaptive = status_dict["adaptive_card"]
    
    print(f"🔄 Phase 1 - Sales Org: {sales['status']} (stream={sales['stream_length']}, pending={sales['pending_count']})")
    print(f"🎯 Phase 2 - Adaptive Card: {adaptive['status']} (stream={adaptive['stream_length']}, pending={adaptive['pending_count']})")
    print("="*60)
