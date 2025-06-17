# src/api_server.py
import yaml # YAML 파일을 읽기 위해 import
from dotenv import load_dotenv # .env 파일을 읽기 위해 import
import google.generativeai as genai
import sys
import uuid
import os
import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional

# 기존에 만들었던 파이프라인 모듈들을 import 합니다.
from Crawling.naver_crawler import run_naver_crawling
from Crawling.kakao_crawler import run_kakao_crawling
from QC_score.score_pipline import run_scoring_pipeline

def load_config(config_path: str) -> dict:
    """YAML 설정 파일을 불러옵니다."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        return {}

# --- 1. FastAPI 앱 초기화 및 설정 ---
app = FastAPI(title="Store Data Pipeline API")

config = load_config("config.yaml")

DATA_DIR = config.get('data_dir', 'data')
OUTPUT_DIR = config.get('output_dir', 'results')
PIPELINE_STAGE = config.get('pipeline_stage', 'full')
NUM_THREADS = config.get('num_threads', 1)
HEADLESS_MODE = config.get('headless_mode', False)
SAVE_INTERVAL = config.get('save_interval', 0)

# 작업의 상태와 결과를 저장할 간단한 인메모리 '데이터베이스'
# (서버가 재시작되면 내용이 사라지므로, 실제 운영 환경에서는 Redis나 DB 사용을 권장합니다)
tasks_db: Dict[str, Dict] = {}

# --- 2. API가 주고받을 데이터 형식 정의 (Pydantic 모델) ---
class StoreInput(BaseModel):
    name: str
    location: str

class PipelineOptions(BaseModel):
    num_threads: int = NUM_THREADS
    headless_mode: bool = HEADLESS_MODE
    save_interval: int = SAVE_INTERVAL


class PipelineRequest(BaseModel):
    stores: List[StoreInput]
    options: Optional[PipelineOptions] = None # 옵션은 필수가 아님

class TaskResponse(BaseModel):
    task_id: str
    message: str

class StatusResponse(BaseModel):
    task_id: str
    status: str
    result: Optional[List[Dict]] = None

# --- 3. 백그라운드에서 실행될 실제 파이프라인 함수 ---
def execute_pipeline_task(task_id: str, stores_to_process: List[Dict], options: Dict):
    """
    오래 걸리는 전체 파이프라인 로직을 수행하는 함수입니다.
    이 함수는 BackgroundTasks에 의해 별도로 실행됩니다.
    """
    try:
        # 작업 상태를 '처리중'으로 업데이트
        tasks_db[task_id] = {"status": "processing: 1. naver crawling", "result": None}
        print(f"[{task_id}] 파이프라인 시작...")

        # [수정] UI 옵션값과 config 기본값을 비교하여 최종 실행값 결정 ▼▼▼
        num_threads_run = options.get('num_threads', NUM_THREADS)
        headless_mode_run = options.get('headless_mode', HEADLESS_MODE)
        save_interval_run = options.get('save_interval', SAVE_INTERVAL)

        print(f"[{task_id}] 실행 옵션 - 스레드: {num_threads_run}, 헤드리스: {headless_mode_run}, 저장간격: {save_interval_run}")


        # run_naver_crawling 함수는 CSV 파일 경로를 인자로 받으므로,
        # API로 받은 가게 목록을 임시 CSV 파일로 저장합니다.
        temp_input_df = pd.DataFrame(stores_to_process)
        temp_csv_path = os.path.join(DATA_DIR, f"temp_input_{task_id}.csv")
        temp_input_df.to_csv(temp_csv_path, index=False, encoding='utf-8-sig')

        # 1단계: 네이버 크롤링
        # 이거 설정을 config.yaml에서 불러오도록 변경
        # ▼▼▼ [수정] 결정된 실행값을 크롤링 함수에 전달 ▼▼▼
        naver_df = run_naver_crawling(
            csv_path=temp_csv_path, 
            num_threads=num_threads_run,
            headless_mode=headless_mode_run,
            save_interval=save_interval_run,
            output_dir=OUTPUT_DIR
        )
        os.remove(temp_csv_path)

        if naver_df.empty: raise ValueError("네이버 크롤링 결과가 없습니다.")
        
        # 2단계: 카카오 크롤링
        tasks_db[task_id]["status"] = "processing: 2. kakao crawling"
        kakao_df = run_kakao_crawling(input_df=naver_df, max_threads=1, headless=True)

        # 3단계: 점수 산정
        tasks_db[task_id]["status"] = "processing: 3. scoring"
        final_data_list = run_scoring_pipeline(input_data=kakao_df.to_dict('records'), data_dir=DATA_DIR)
        
        # 4단계: 최종 결과 저장
        tasks_db[task_id]["status"] = "completed"
        tasks_db[task_id]["result"] = final_data_list
        print(f"[{task_id}] 파이프라인 성공적으로 완료.")

    except Exception as e:
        # 오류 발생 시 상태를 'failed'로 변경
        tasks_db[task_id]["status"] = "failed"
        tasks_db[task_id]["result"] = {"error": str(e)}
        print(f"[{task_id}] 파이프라인 실행 중 오류 발생: {e}")


# --- 4. API 서버 시작 시 단 한번 실행될 이벤트 ---
@app.on_event("startup")
def on_startup():
    """API 서버가 시작될 때 API 키를 설정하고 폴더를 생성합니다."""
    load_dotenv(dotenv_path=".config.env")
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY를 찾을 수 없습니다. .env 파일을 확인하세요.")
    
    try:
        API_KEY = os.getenv("GOOGLE_API_KEY")
        genai.configure(api_key=API_KEY)
        list(genai.list_models())
        print("✅ Google Gemini API 키가 성공적으로 검증되었습니다.")
    except Exception as e:
        raise RuntimeError(f"API 키가 유효하지 않습니다: {e}")

    # 필요한 폴더 생성
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("API 서버가 시작되었습니다.")
# --- 5. API 엔드포인트(URL 주소) 구현 ---

# 초기 config 값을 반환
@app.get("/config")
async def get_config():
    """UI가 초기값을 설정할 수 있도록 서버의 기본 설정을 반환합니다."""
    return {
        "num_threads": NUM_THREADS,
        "headless_mode": HEADLESS_MODE,
        "save_interval": SAVE_INTERVAL,
        "data_dir": DATA_DIR,
        "output_dir": OUTPUT_DIR,
        "pipeline_stage": PIPELINE_STAGE,
    }


@app.post("/run-pipeline", response_model=TaskResponse, status_code=202)
async def start_pipeline_endpoint(request: PipelineRequest, background_tasks: BackgroundTasks):
    """
    파이프라인 실행을 요청하고 즉시 '진동벨'(작업 ID)을 반환합니다.
    """
    if not request.stores:
        raise HTTPException(status_code=400, detail="매장 목록이 비어있습니다.")

    task_id = str(uuid.uuid4()) # 고유한 작업 ID 생성
    tasks_db[task_id] = {"status": "pending", "result": None}
    
    # ▼▼▼ [수정] 요청에서 옵션을 추출하고 백그라운드 함수에 전달 ▼▼▼
    request_options = request.options.dict() if request.options else {}
    
    # execute_pipeline_task 함수를 백그라운드에서 실행하도록 등록
    background_tasks.add_task(
        execute_pipeline_task, 
        task_id, 
        request.dict()["stores"],
        request_options # options 전달
    )
    
    return {"task_id": task_id, "message": "파이프라인 작업이 접수되었습니다. 잠시 후 상태 확인 API를 통해 결과를 조회하세요."}


@app.get("/status/{task_id}", response_model=StatusResponse)
async def get_task_status_endpoint(task_id: str):
    """
    주어진 작업 ID의 현재 상태와 결과를 확인합니다.
    """
    task = tasks_db.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="해당 작업 ID를 찾을 수 없습니다.")
    
    return {"task_id": task_id, **task}