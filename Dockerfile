# 1. 베이스 이미지 선택
# Debian 운영체제 기반의 파이썬 3.12 이미지를 사용합니다.
# 'slim' 버전보다 용량이 크지만, apt-get을 통한 패키지 설치가 용이합니다.
FROM python:3.12-bookworm

# 2. 시스템 의존성 및 Firefox 설치
# Selenium 구동에 필요한 Firefox 브라우저와 관련 도구를 설치합니다.
RUN apt-get update && apt-get install -y \
    wget \
    tar \
    firefox-esr \
    && rm -rf /var/lib/apt/lists/*

# 3. GeckoDriver (Firefox용 웹 드라이버) 설치
# GitHub에서 최신 버전의 GeckoDriver를 찾아 자동으로 다운로드 및 설치합니다.
RUN GECKODRIVER_VERSION=$(wget -q -O - "https://api.github.com/repos/mozilla/geckodriver/releases/latest" | grep '"tag_name":' | sed -E 's/.*"v([^"]+)".*/\1/') && \
    wget --no-verbose -O /tmp/geckodriver.tar.gz "https://github.com/mozilla/geckodriver/releases/download/v${GECKODRIVER_VERSION}/geckodriver-v${GECKODRIVER_VERSION}-linux64.tar.gz" && \
    tar -C /usr/local/bin -xvf /tmp/geckodriver.tar.gz && \
    chmod +x /usr/local/bin/geckodriver && \
    rm /tmp/geckodriver.tar.gz

# 4. 작업 디렉토리 설정
# 컨테이너 내부에서 명령어가 실행될 기본 경로를 지정합니다.
WORKDIR /app

# 5. 파이썬 라이브러리 설치
# 먼저 requirements.txt 파일만 복사하여 라이브러리를 설치합니다.
# => 이렇게 하면 소스 코드만 변경되었을 때, 매번 라이브러리를 다시 설치하지 않고 캐시된 레이어를 사용해 빌드 속도가 매우 빨라집니다.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. 소스 코드 복사
# 현재 폴더의 모든 프로젝트 파일들(api_server.py, config.yaml, Crawling/ 등)을 컨테이너 안으로 복사합니다.
COPY . .

# 7. 포트 노출
# 컨테이너 외부에서 8000번 포트로 접속할 수 있도록 설정합니다.
EXPOSE 8000

# 8. 컨테이너 실행 명령어
# 컨테이너가 시작될 때 uvicorn을 이용해 FastAPI 서버를 실행합니다.
# '--host 0.0.0.0'은 컨테이너 외부에서의 접속을 허용하기 위해 필수입니다.
CMD ["uvicorn", "src.api_server:app", "--host", "0.0.0.0", "--port", "8000"]
