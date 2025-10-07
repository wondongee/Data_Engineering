#!/bin/bash

# Scalable Pipeline 실행 스크립트

echo "🚀 Starting Scalable Adaptive Card Pipeline..."

# 환경변수 설정
export MAX_WORKERS_SALES=${MAX_WORKERS_SALES:-3}
export MAX_WORKERS_ADAPTIVE=${MAX_WORKERS_ADAPTIVE:-3}
export REDIS_URL=${REDIS_URL:-"redis://localhost:6379/0"}

echo "📊 Configuration:"
echo "  - Sales Org Workers: $MAX_WORKERS_SALES"
echo "  - Adaptive Card Workers: $MAX_WORKERS_ADAPTIVE"
echo "  - Redis URL: $REDIS_URL"

# Python 경로 설정
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# 수평 확장 가능한 스케줄러 실행
echo "🔄 Starting scalable scheduler..."
python -m app.scalable_scheduler
