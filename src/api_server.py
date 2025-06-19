# api_server.py (최종 완성본)

import os
import sys
import uuid
import re
import yaml
import pandas as pd
import traceback
import subprocess
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Optional
from datetime import datetime
import google.generativeai as genai
from dotenv import load_dotenv

# 기존에 만들었던 파이프라인 모듈들을 import합니다.
# 이 함수들이 api_server.py와 같은 레벨의 폴더에 있다고 가정합니다.
from Crawling.naver_crawler import run_naver_crawling
from Crawling.kakao_crawler import run_kakao_crawling
from QC_score.score_pipline import run_scoring_pipeline

# --- 1. 설정 및 전역 변수 초기화 ---
app = FastAPI(title="Store Data Pipeline API")

# 설정 파일 로드
try:
    config = yaml.safe_load(open("config.yaml", 'r', encoding='utf-8'))
except FileNotFoundError:
    print("오류: config.yaml 파일을 찾을 수 없습니다. 기본값으로 실행됩니다.")
    config = {}

# .env 파일에서 API 키 로드
load_dotenv(dotenv_path=".config.env")

# 작업 상태 및 결과를 저장할 인메모리 DB
tasks_db: Dict[str, Dict] = {}
# 서버 세션 동안 중복 ID를 관리할 set (서버 재시작 시 초기화됨)
CRAWLED_IDS_IN_SESSION: set = set()

# --- 2. API 데이터 형식 정의 (Pydantic) ---
class PipelineRequest(BaseModel):
    query: str = Field(..., description="크롤링할 검색어 (필수)", example="성수동 카페")
    latitude: Optional[float] = Field(None, description="검색 기준점 위도 (선택)", example=37.544)
    longitude: Optional[float] = Field(None, description="검색 기준점 경도 (선택)", example=127.044)
    show_browser: bool = Field(False, description="크롤링 브라우저 창 표시 여부 (디버깅용)")

class TaskResponse(BaseModel):
    task_id: str
    message: str

class StatusResponse(BaseModel):
    task_id: str
    status: str
    result_path: Optional[str] = None
    error: Optional[str] = None

# --- 3. 핵심 파이프라인 실행 함수 ---
def execute_pipeline_task(task_id: str, request: PipelineRequest):
    """오래 걸리는 전체 파이프라인 로직을 수행하는 함수 (백그라운드 실행용)"""
    output_dir = ""
    try:
        tasks_db[task_id] = {"status": "processing", "result_path": None, "error": None}
        print(f"[{task_id}] 파이프라인 시작: query='{request.query}'")

        safe_query_name = re.sub(r'[\\/*?:"<>|]', "", request.query)
        run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = os.path.join(config.get('output_dir', 'results'), f"{safe_query_name}_{task_id[:3]}_{run_timestamp}")
        os.makedirs(output_dir, exist_ok=True)
        
        # 1. Naver Crawling
        tasks_db[task_id]["status"] = "processing: naver crawling"
        naver_df = run_naver_crawling(
            search_query=request.query,
            latitude=request.latitude,
            longitude=request.longitude,
            headless_mode=(not request.show_browser),
            output_dir=output_dir,
            existing_naver_ids=CRAWLED_IDS_IN_SESSION
        )
        if naver_df.empty: raise ValueError("네이버 크롤링 결과가 없습니다.")
        
        newly_crawled_ids = set(naver_df['naver_id'].dropna().unique())
        CRAWLED_IDS_IN_SESSION.update(newly_crawled_ids)
        print(f"[{task_id}] 네이버 크롤링 완료. 현재까지 누적 ID 개수: {len(CRAWLED_IDS_IN_SESSION)}")

        # 2. Kakao Crawling
        tasks_db[task_id]["status"] = "processing: kakao crawling"
        kakao_df = run_kakao_crawling(
            input_df=naver_df,
            max_threads=config.get('num_threads', 3),
            headless=(not request.show_browser)
        )

        # 3. Scoring
        tasks_db[task_id]["status"] = "processing: scoring"
        final_list = run_scoring_pipeline(
            input_data=kakao_df.to_dict('records'),
            data_dir=config.get('data_dir', 'data')
        )
        if not final_list: raise ValueError("점수 산정 결과가 없습니다.")

        final_df = pd.DataFrame(final_list)
        final_output_path = os.path.join(output_dir, "final_result.json")
        final_df.to_json(final_output_path, orient='records', force_ascii=False, indent=2)
        
        tasks_db[task_id].update({"status": "completed", "result_path": final_output_path})
        print(f"[{task_id}] 파이프라인 성공. 결과: {final_output_path}")

    except Exception as e:
        error_message = str(e)
        tasks_db[task_id].update({"status": "failed", "error": error_message})
        print(f"[{task_id}] 파이프라인 오류: {error_message}")
        if output_dir:
            error_log_path = os.path.join(output_dir, "error_log.txt")
            with open(error_log_path, 'w', encoding='utf-8') as f:
                f.write(f"Error: {error_message}\n\n")
                f.write(traceback.format_exc())

# --- 4. 서버 시작 시 실행될 이벤트 ---
def clean_firefox_cache():
    """Firefox 관련 캐시 파일을 정리합니다."""
    try:
        print("🧹 /tmp 내 Firefox 캐시 파일 정리 시도...")
        if sys.platform != "win32": # 윈도우가 아닐 경우에만 실행
            subprocess.run("rm -rf /tmp/rust_mozprofile* /tmp/Temp-*profile /tmp/geckodriver*", shell=True, check=False)
            print("✅ 캐시 파일 정리 완료.")
        else:
            print("- 윈도우 환경에서는 캐시 정리를 건너뜁니다.")
    except Exception as e:
        print(f"⚠️ 캐시 파일 정리 중 오류 발생: {e}")

def setup_api_key():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("경고: GOOGLE_API_KEY를 찾을 수 없습니다. Scoring 단계가 실패할 수 있습니다.")
        return
    genai.configure(api_key=api_key)
    print("✅ Google Gemini API 키가 설정되었습니다.")

@app.on_event("startup")
def on_startup():
    print("🚀 API 서버 시작...")
    setup_api_key()
    clean_firefox_cache()

# --- 5. API 엔드포인트 구현 ---
@app.post("/pipeline/run", response_model=TaskResponse, status_code=202)
async def start_pipeline_endpoint(request: PipelineRequest, background_tasks: BackgroundTasks):
    """파이프라인 실행을 요청하고 즉시 작업 ID를 반환합니다."""
    task_id = str(uuid.uuid4())
    tasks_db[task_id] = {"status": "pending"}
    
    background_tasks.add_task(execute_pipeline_task, task_id, request)
    
    return {"task_id": task_id, "message": "파이프라인 작업이 접수되었습니다. '/pipeline/status/{task_id}'로 상태를 확인하세요."}

@app.get("/pipeline/status/{task_id}", response_model=StatusResponse)
async def get_task_status_endpoint(task_id: str):
    """주어진 작업 ID의 현재 상태와 결과를 확인합니다."""
    task = tasks_db.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="해당 작업 ID를 찾을 수 없습니다.")
    
    return {"task_id": task_id, **task}