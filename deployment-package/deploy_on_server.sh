#!/bin/bash

# 서버에서 실행할 배포 스크립트

set -e

echo "🚀 오프라인 서버 배포를 시작합니다..."

# 도커 이미지 로드
if [ -f "data-engineering-image.tar.gz" ]; then
    echo "📦 도커 이미지를 로드합니다..."
    gunzip -c data-engineering-image.tar.gz | docker load
    echo "✅ 도커 이미지 로드 완료"
fi

# 환경 변수 설정 (필요한 경우)
export REDIS_URL=${REDIS_URL:-"redis://localhost:6379/0"}
export DB_HOST=${DB_HOST:-"localhost"}
export DB_PORT=${DB_PORT:-"5432"}
export DB_NAME=${DB_NAME:-"adaptive_card_db"}
export DB_USER=${DB_USER:-"postgres"}
export DB_PASSWORD=${DB_PASSWORD:-"password"}
export TZ=${TZ:-"Asia/Seoul"}
export CSV_OUTPUT_DIR=${CSV_OUTPUT_DIR:-"./output"}

# 출력 디렉토리 생성
mkdir -p output

# 도커 컨테이너 실행
echo "🐳 도커 컨테이너를 실행합니다..."
docker run -d \
    --name data-engineering-app \
    --restart unless-stopped \
    -p 8000:8000 \
    -e REDIS_URL="$REDIS_URL" \
    -e DB_HOST="$DB_HOST" \
    -e DB_PORT="$DB_PORT" \
    -e DB_NAME="$DB_NAME" \
    -e DB_USER="$DB_USER" \
    -e DB_PASSWORD="$DB_PASSWORD" \
    -e TZ="$TZ" \
    -e CSV_OUTPUT_DIR="$CSV_OUTPUT_DIR" \
    -v $(pwd)/output:/app/output \
    data-engineering:latest

# 컨테이너 상태 확인
echo "📊 컨테이너 상태를 확인합니다..."
docker ps | grep data-engineering-app

echo "🎉 배포가 완료되었습니다!"
echo "📋 사용 가능한 명령어:"
echo "  - 컨테이너 상태 확인: docker ps"
echo "  - 로그 확인: docker logs -f data-engineering-app"
echo "  - 컨테이너 중지: docker stop data-engineering-app"
echo "  - 컨테이너 재시작: docker restart data-engineering-app"
