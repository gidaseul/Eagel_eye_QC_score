[프로젝트 설명서 다운로드 (PDF)](./매의 눈 프로젝트 정리본.pdf)
<br/>
<br/>

# 0. Getting Started (시작하기)

**API 서버 실행 방법**
```bash
uvicorn src/api_server:app --reload
```

<br/>
<br/>

# 1. Project Overview (프로젝트 개요)

- 프로젝트 이름: 매의 눈 프로젝트
- 프로젝트 설명: 본 프로젝트는 영업팀 및 QC 검수팀의 제휴 제안 매장 선별의 편의성을 위해 네이버 지도와 카카오맵 데이터를 수집합니다. 이후 구글의 Gemini 모델을 활용하여 데이트팝의 제휴 가능한 매장의 기준을 판단하여 QC Score를 산출합니다. (Notion에 자세히 공유되어 있습니다.)
  - **프로젝트 진행 기간**: 2025.03.01 ~ 06.30
  - **목표**: LLM 기반의 분석 파이프라인을 통해 네이버와 카카오맵의 분산된 데이터를 하나의 정량적 QC 스코어로 통합하여, 자동화된 1차 필터링 시스템을 구축하고 업무 리소스 부담을 줄이는 것을 목표로 합니다.

<br/>
<br/>

# 2. 전체 동작 파이프라인

- **네이버 지도**
  - **수집하는 내용**: 매장 이름, 주소, 전화번호, GPS 정보(위도, 경도) 등의 기본 정보. 방문자 리뷰 수, 블로그 리뷰 수, 리뷰 키워드 및 개수, 테마 키워드(분위기, 주제, 목적) 등의 인기도 정보. 인스타그램 링크, 게시물 수, 팔로워 수. 지하철역과의 거리, TV 방영 여부, 주차 가능 여부, 서울 미쉐린 가이드 선정 여부, 20-30대 방문 비율, 성별 비율, 활발한 운영 지표. 메뉴 목록(이름, 가격, 대표 메뉴 여부, TV 방송 여부, 메뉴 소개). 개별 리뷰 정보(날짜, 내용).
  - <img src="https://github.com/user-attachments/assets/5b7156b9-6bd5-48e7-839b-b2c06dce7ad8" alt="네이버 지도" width="300" />

- **카카오 지도**
  - **수집하는 내용**: 전체 별점(`kakao_score`), 후기 개수(`kakao_review`), 맛(`kakao_taste`), 가성비(`kakao_value`), 친절도(`kakao_kindness`), 분위기(`kakao_mood`), 주차 편의성(`kakao_parking`) 점수.
  - <img src="https://github.com/user-attachments/assets/7eb1d9e5-1f74-42e3-a136-f36f114ce93c" alt="카카오 지도" width="300" />

- **LLM (구글 Gemini)**
  - 네이버, 카카오 데이터를 기반으로 프롬프트 가이드라인을 통해 데이트팝의 제휴 기준 충족 여부를 판별하고 점수 및 산출 근거를 제공합니다. 특히 단순 평점으로 파악하기 어려운 리뷰의 잠재 의미와 맥락 해석을 위한 **의미론적 분석**을 수행합니다.
  -   - <img src="https://github.com/user-attachments/assets/67b6dde0-9270-480d-82a9-9506f41b86d4" alt="구글 Gemini" width="300" />

<br/>
<br/>

# 3. 코드 동작 설명서

- **API 서버 실행 방법**: `uvicorn src/api_server:app --reload`
  - API 서버는 `http://127.0.0.1:8000`에서 실행되며, `http://127.0.0.1:8000/docs`에서 Swagger UI로 API 문서를 확인할 수 있습니다.
  - **`POST /pipeline/run`**: 파이프라인 실행을 요청하고 작업 ID를 반환. 파라미터로 `storage_mode`, `query`, `latitude`, `longitude`, `zoom_level`, `show_browser` 등을 사용합니다.
  - **`POST /pipeline/target-run`**: 특정 매장명과 주소를 바탕으로 일치하는 것을 선택하기 위한 파이프라인 실행을 요청하고 작업 ID를 반환. 파라미터로 `storage_mode`, `query`, `latitude`, `longitude`, `zoom_level`, `show_browser` 등을 사용합니다.
  - **`GET /pipelines/status/{task_id}`**: 특정 작업 ID의 상태, 진행 단계, 결과 경로, 오류 메시지 등을 조회합니다.
  - **`GET /config`**: 서버 로드 설정 전체를 조회합니다.
  - **`POST /admin/consolidation`**: 저장된 각 파일을 병합하여 최신 마스터 파일로 만드는 배치 작업을 실행합니다. 이때 master 파일을 병합한 것을 기준으로 파이프라인이 실행될 때 중복된 것을 확인하며 실행을 합니다. (NAVER_ID를 기준으로 중복 체크 확인 합니다.)


<br/>
<br/>

# 4. 기술 스택

## 4.1 Language (Python)

| 언어   | 설명                                                                 |
| ------ | -------------------------------------------------------------------- |
| Python | 데이터 수집, 처리, 분석, API 서버, UI 프론트엔드까지 Python으로 통일 |

<br/>

## 4.2 Backend

| 기술       | 설명                                                                                                                                                                                                                       |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| FastAPI    | 비동기 API 서버. `BackgroundTasks`로 장기 작업을 처리하여 즉시 작업 접수 응답과 작업 ID를 반환하고, 실제 작업은 백그라운드에서 수행. HTTP 타임아웃 방지.                                                                  |
| Uvicorn    | FastAPI용 ASGI 서버.                                                                                                                                                                                                       |
| Docker     | 모든 환경을 이미지로 패키징하여 의존성 문제 해결 및 배포 용이성 확보.                                                                                                               |
| Docker Compose | Nginx, Gunicorn/FastAPI 등 여러 컨테이너를 통합 관리.                                                                                                                               |
| Gunicorn   | FastAPI용 WSGI 서버.                                                                                                                                                                                                       |
| Nginx      | 리버스 프록시로 사용되어 안정성과 성능 확보.                                                                                                                                        |
| Google Generative AI (Gemini) | 메뉴 카테고리 판단, 의미론적 해석 기반 분석 및 점수화에 사용.                                                                                              |
| Pandas     | 데이터 핸들링 표준 라이브러리.                                                                                                                                                       |
| Selenium   | 동적 웹 페이지 크롤링.                                                                                                                                                               |
| RapidFuzz  | 문자열 유사도 계산.                                                                                                                                                                  |
| Levenshtein | 문자열 편집 거리 계산.                                                                                                                                                             |
| Shapely    | 지리 공간 데이터 Polygon 처리 및 분석.                                                                                                                                            |
| boto3      | AWS S3 파일 저장 및 다운로드 주소 반환.                                                                                                                                             |
| python-dotenv | 환경 변수 관리.                                                                                                                                                                    |

<br/>

## 4.3 Cooperation

| 툴      | 설명                                                                                                                                                                  |
| ------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Git     | 분산 버전 관리 시스템. <img src="https://github.com/user-attachments/assets/483abc38-ed4d-487c-b43a-3963b33430e6" alt="git" width="100" /> |
| GitHub  | Git 기반 웹 호스팅 서비스. Issues, Projects 기능으로 버전 관리 및 협업 진행.                                                 |
| Notion  | 협업 툴. <img src="https://github.com/user-attachments/assets/34141eb9-deca-416a-a83f-ff9543cc2f9a" alt="Notion" width="100" /> |
| Discord | <!-- TODO: 작성 -->                                                                                                           |

<br/>

# 5. Project Structure (프로젝트 구조)

```plaintext
project/
├── .gitignore               # Git 무시 파일 목록
├── README.md                # 프로젝트 개요 및 사용법
├── batch_consolidate.py     # 수집 데이터 통합 배치 스크립트
├── config.yaml              # 프로젝트 설정 파일
├── docker-compose.yml       # Docker Compose 설정
├── main_pipeline.py         # 파이프라인 실행 메인 스크립트
├── requirements.txt         # Python 종속성 목록
├── Crawling/                # 크롤러 관련 소스 코드
│   ├── kakao_crawler.py
│   ├── naver_crawler.py
│   ├── naver_crawler_detail.py
│   ├── naver_crawler_target.py
│   └── utils/
│       ├── check_franchise.py
│       ├── convert_str_to_number.py
│       ├── extract_store_info.py
│       ├── get_instagram_link.py
│       ├── haversine.py
│       ├── is_within_date.py
│       ├── load_bluer.py
│       ├── logger_utils.py
│       └── master_loader.py
├── QC_score/
│   ├── polygon_update.ipynb
│   ├── score_pipline.py
├── Score/
│   ├── LLM_gemini.ipynb
│   └── QC_Center_score.ipynb
└── src/
    ├── api_server.py
    └── ui_app.py
```

<br/>
<br/>

# 6. 브랜치 설명 (Branch Strategy)

- **Main Branch**
  - 이전 프로젝트의 브랜치로, 기준점 없이 인기도 기반 유사도 측정으로 매장을 선별.
  - 모든 배포는 이 브랜치에서 진행.

- **Pipeline Branch**
  - 현재 FastAPI 기반의 새 스코어 예측 파이프라인 설계가 진행되는 브랜치.

<br/>
<br/>
