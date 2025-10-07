"""
설정 관리: 환경변수를 통한 설정 로드
"""

import os
from typing import List

# ---------- helpers ----------
def _get_required(name: str) -> str:
   v = os.getenv(name)
   if v is None or v == "":
       raise RuntimeError(f"Required environment variable '{name}' is missing")
   return v
 
def _get_int(name: str, default: int) -> int:
   v = os.getenv(name)
   return int(v) if v not in (None, "") else default
 
# ---------- 기본 설정 ----------
TZ = os.getenv("TZ", "Asia/Seoul")

# ---------- Redis ----------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# ---------- 큐 설정 ----------
SALES_ORG_QUEUE = os.getenv("SALES_ORG_QUEUE", "sales_org_queue")
ADAPTIVE_CARD_QUEUE = os.getenv("ADAPTIVE_CARD_QUEUE", "adaptive_card_queue")
SALES_ORG_GROUP = os.getenv("SALES_ORG_GROUP", "sales_org_group")
ADAPTIVE_CARD_GROUP = os.getenv("ADAPTIVE_CARD_GROUP", "adaptive_card_group")

# ---------- 워커 설정 ----------
CRON_SPEC = os.getenv("CRON_SPEC", "0 */2 * * *")
SALES_ORG_WORKER_NAME = os.getenv("SALES_ORG_WORKER_NAME", "sales_org_worker")
ADAPTIVE_CARD_WORKER_NAME = os.getenv("ADAPTIVE_CARD_WORKER_NAME", "adaptive_card_worker")
BATCH_COUNT = _get_int("BATCH_COUNT", 10)

# ---------- API 설정 ----------
T2A_API_URL = os.getenv("T2A_API_URL", "https://api.t2a.example.com")
T2A_API_KEY = os.getenv("T2A_API_KEY", "your_api_key_here")
T2A_TIMEOUT_SEC = _get_int("T2A_TIMEOUT_SEC", 10)

# ---------- 데이터베이스 설정 ----------
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'adaptive_card_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')

# ---------- 파일 경로 ----------
CSV_OUTPUT_DIR = os.getenv('CSV_OUTPUT_DIR', './output')