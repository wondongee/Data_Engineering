# app/retry_handler.py

import time
import logging
from typing import Callable, Any, Optional
from functools import wraps
import redis

log = logging.getLogger(__name__)

class RetryHandler:
    """
    Redis Stream 메시지 처리를 위한 재시도 핸들러
    """
    
    def __init__(self, redis_client: redis.Redis, stream: str, group: str, consumer: str):
        self.r = redis_client
        self.stream = stream
        self.group = group
        self.consumer = consumer
        
    def process_with_retry(
        self, 
        message_id: str, 
        fields: dict, 
        handler_func: Callable,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0
    ) -> bool:
        """
        메시지를 재시도 로직과 함께 처리
        
        Args:
            message_id: Redis Stream 메시지 ID
            fields: 메시지 필드들
            handler_func: 실제 처리 함수
            max_retries: 최대 재시도 횟수
            base_delay: 기본 지연 시간 (초)
            max_delay: 최대 지연 시간 (초)
            
        Returns:
            bool: 처리 성공 여부
        """
        
        for attempt in range(max_retries + 1):
            try:
                # 메시지 처리 시도
                handler_func(message_id, fields)
                
                # 성공 시 ACK
                self.r.xack(self.stream, self.group, message_id)
                log.info(f"[RETRY] Message {message_id} processed successfully on attempt {attempt + 1}")
                return True
                
            except Exception as e:
                log.warning(f"[RETRY] Attempt {attempt + 1} failed for message {message_id}: {e}")
                
                if attempt < max_retries:
                    # 지수 백오프로 대기
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    log.info(f"[RETRY] Waiting {delay:.2f}s before retry {attempt + 2}")
                    time.sleep(delay)
                else:
                    # 최대 재시도 횟수 초과
                    log.error(f"[RETRY] Max retries exceeded for message {message_id}. Moving to dead letter queue.")
                    self._move_to_dead_letter_queue(message_id, fields, str(e))
                    return False
        
        return False
    
    def _move_to_dead_letter_queue(self, message_id: str, fields: dict, error_msg: str):
        """실패한 메시지를 Dead Letter Queue로 이동"""
        try:
            dlq_fields = {
                **fields,
                "original_message_id": message_id,
                "error_message": error_msg,
                "failed_at": time.time(),
                "retry_count": "max_exceeded"
            }
            
            # Dead Letter Queue에 메시지 추가
            self.r.xadd(f"{self.stream}:dlq", dlq_fields)
            log.info(f"[DLQ] Message {message_id} moved to dead letter queue")
            
        except Exception as e:
            log.error(f"[DLQ] Failed to move message {message_id} to DLQ: {e}")

def retry_on_failure(max_retries: int = 3, base_delay: float = 1.0):
    """
    함수 데코레이터: 함수 실행 실패 시 재시도
    
    Args:
        max_retries: 최대 재시도 횟수
        base_delay: 기본 지연 시간 (초)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    log.warning(f"[RETRY] Function {func.__name__} failed on attempt {attempt + 1}: {e}")
                    
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), 60.0)
                        log.info(f"[RETRY] Waiting {delay:.2f}s before retry {attempt + 2}")
                        time.sleep(delay)
                    else:
                        log.error(f"[RETRY] Max retries exceeded for function {func.__name__}")
                        raise last_exception
            
            raise last_exception
        
        return wrapper
    return decorator

def get_pending_messages_with_retry_info(redis_client: redis.Redis, stream: str, group: str) -> list:
    """
    Pending 상태의 메시지들을 재시도 정보와 함께 조회
    
    Returns:
        list: [{"message_id": str, "consumer": str, "idle_time": int, "retry_count": int}, ...]
    """
    try:
        pending_info = redis_client.xpending_range(stream, group, min="-", max="+", count=100)
        
        messages_with_retry = []
        for msg_info in pending_info:
            # 메시지의 retry_count 정보 추출 (메시지 필드에서)
            message_id = msg_info["message_id"]
            try:
                # 메시지 필드 조회
                messages = redis_client.xrange(stream, min=message_id, max=message_id)
                if messages:
                    fields = messages[0][1]
                    retry_count = int(fields.get("retry_count", "0"))
                else:
                    retry_count = 0
            except:
                retry_count = 0
            
            messages_with_retry.append({
                "message_id": message_id,
                "consumer": msg_info["consumer"],
                "idle_time": msg_info["time_since_delivered"],
                "retry_count": retry_count
            })
        
        return messages_with_retry
        
    except Exception as e:
        log.error(f"Failed to get pending messages: {e}")
        return []

def cleanup_old_pending_messages(redis_client: redis.Redis, stream: str, group: str, max_idle_time: int = 3600):
    """
    오래된 pending 메시지들을 정리 (1시간 이상 idle)
    
    Args:
        max_idle_time: 최대 idle 시간 (밀리초)
    """
    try:
        pending_messages = get_pending_messages_with_retry_info(redis_client, stream, group)
        
        for msg_info in pending_messages:
            if msg_info["idle_time"] > max_idle_time:
                message_id = msg_info["message_id"]
                log.warning(f"[CLEANUP] Claiming old pending message {message_id}")
                
                # 메시지를 현재 consumer로 claim
                redis_client.xclaim(
                    stream, group, redis_client.get_connection_kwargs()["client_name"], 
                    max_idle_time, message_id
                )
                
    except Exception as e:
        log.error(f"Failed to cleanup old pending messages: {e}")
