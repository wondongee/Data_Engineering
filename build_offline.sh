#!/bin/bash

# 오프라인 서버용 도커 이미지 빌드 및 내보내기 스크립트

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 이미지 빌드
build_image() {
    log_info "도커 이미지를 빌드합니다..."
    
    # 기존 이미지 제거 (선택적)
    if [ "$1" = "--clean" ]; then
        log_info "기존 이미지를 제거합니다..."
        docker rmi data-engineering:latest 2>/dev/null || true
    fi
    
    # 도커 이미지 빌드
    docker build -t data-engineering:latest .
    
    if [ $? -eq 0 ]; then
        log_success "도커 이미지 빌드가 완료되었습니다!"
    else
        log_error "도커 이미지 빌드에 실패했습니다."
        exit 1
    fi
}

# 이미지를 tar.gz 파일로 내보내기
export_image() {
    local output_file="data-engineering-image.tar.gz"
    
    log_info "도커 이미지를 tar.gz 파일로 내보냅니다..."
    
    # 이미지 내보내기 및 압축
    docker save data-engineering:latest | gzip > $output_file
    
    if [ $? -eq 0 ]; then
        log_success "이미지 내보내기가 완료되었습니다: $output_file"
        
        # 파일 크기 확인
        local file_size=$(du -h $output_file | cut -f1)
        log_info "파일 크기: $file_size"
        
    else
        log_error "이미지 내보내기에 실패했습니다."
        exit 1
    fi
}

# 배포 패키지 생성
create_deployment_package() {
    local package_dir="deployment-package"
    
    log_info "배포 패키지를 생성합니다..."
    
    # 배포 디렉토리 생성
    mkdir -p $package_dir
    
    # 필요한 파일들 복사
    cp -r app/ $package_dir/
    cp -r src/ $package_dir/
    cp requirements.txt $package_dir/
    cp README.md $package_dir/
    
    # 도커 이미지 파일 복사
    if [ -f "data-engineering-image.tar.gz" ]; then
        cp data-engineering-image.tar.gz $package_dir/
    fi
    
    # 서버용 배포 스크립트 생성
    cat > $package_dir/deploy_on_server.sh << 'EOF'
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
EOF
    
    chmod +x $package_dir/deploy_on_server.sh
    
    # 패키지 압축
    log_info "배포 패키지를 압축합니다..."
    tar -czf deployment-package.tar.gz $package_dir/
    
    log_success "배포 패키지가 생성되었습니다: deployment-package.tar.gz"
    
    # 패키지 내용 확인
    log_info "패키지 내용:"
    tar -tzf deployment-package.tar.gz | head -20
    echo "..."
}

# 메인 함수
main() {
    case "$1" in
        "build")
            build_image "$2"
            ;;
        "export")
            export_image
            ;;
        "package")
            create_deployment_package
            ;;
        "all")
            build_image "$2"
            export_image
            create_deployment_package
            ;;
        "help"|"--help"|"-h")
            echo "사용법: $0 [옵션]"
            echo ""
            echo "옵션:"
            echo "  build [--clean]    도커 이미지를 빌드합니다"
            echo "  export             이미지를 tar.gz 파일로 내보냅니다"
            echo "  package            배포 패키지를 생성합니다"
            echo "  all [--clean]      모든 작업을 순차적으로 실행합니다"
            echo "  help               이 도움말을 표시합니다"
            echo ""
            echo "예시:"
            echo "  $0 all --clean     # 전체 프로세스 실행"
            echo "  $0 build           # 이미지만 빌드"
            echo "  $0 export          # 이미지만 내보내기"
            echo "  $0 package         # 패키지만 생성"
            ;;
        "")
            log_error "옵션을 지정해주세요."
            echo "사용법: $0 help"
            exit 1
            ;;
        *)
            log_error "알 수 없는 옵션: $1"
            echo "사용법: $0 help"
            exit 1
            ;;
    esac
}

# 스크립트 실행
main "$@"
