# 1. 기반 이미지 선택: 가볍고 안정적인 공식 파이썬 3.10 이미지를 기반으로 시작합니다.
FROM python:3.10-slim

# 2. 시스템 프로그램 설치: 크롤링에 필수적인 Firefox 브라우저와 드라이버 다운로드를 위한 wget을 설치합니다.
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    firefox-esr \
    && rm -rf /var/lib/apt/lists/* \
    && wget https://github.com/mozilla/geckodriver/releases/download/v0.34.0/geckodriver-v0.34.0-linux-aarch64.tar.gz \
    && tar -zxf geckodriver-v0.34.0-linux-aarch64.tar.gz -C /usr/local/bin \
    && rm geckodriver-v0.34.0-linux-aarch64.tar.gz
    
# 4. 작업 공간 설정: 컨테이너 내부의 /app 폴더를 기본 작업 공간으로 지정합니다.
WORKDIR /app

# 5. 파이썬 라이브러리 설치 (최적화): 코드 전체를 복사하기 전에 requirements.txt만 먼저 복사하여 설치합니다.
#    => 이렇게 하면 소스 코드만 변경되었을 때, 매번 라이브러리를 다시 설치하지 않고 캐시된 레이어를 사용해 빌드 속도가 매우 빨라집니다.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip install gunicorn

# 6. 소스 코드 복사: 현재 폴더의 모든 프로젝트 파일들을 컨테이너 안으로 복사합니다.
COPY . .

# 7. 포트 노출: 이 컨테이너가 내부적으로 8000번 포트를 사용한다고 외부에 알립니다.
EXPOSE 8000

# 8. 실행 명령어: 컨테이너가 시작될 때 최종적으로 실행할 명령입니다.
#    => 운영 환경에 더 적합한 Gunicorn 서버를 이용해 src 폴더 안의 api_server.py를 실행시킵니다.
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "src.api_server:app", "-b", "0.0.0.0:8000"]