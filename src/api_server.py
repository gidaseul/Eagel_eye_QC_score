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
from typing import Dict, Optional, Any
from datetime import datetime
import google.generativeai as genai
from dotenv import load_dotenv
import boto3


# 기존에 만들었던 파이프라인 모듈들을 import합니다.
from Crawling.naver_crawler import run_naver_crawling, run_target_naver_crawling
from Crawling.kakao_crawler import run_kakao_crawling
from QC_score.score_pipline import run_scoring_pipeline
from Crawling.utils.master_loader import load_ids_from_master_data
from batch_consolidate import run_consolidation_job # 배치 작업 함수 import
from copy import deepcopy

# --- 1. 설정 및 전역 변수 초기화 ---
app = FastAPI(title="Store Data Pipeline API")
CONSOLIDATION_IN_PROGRESS = False # 통합 작업 중복 실행 방지 플래그


# 설정 파일 로드
try:
    config = yaml.safe_load(open("config.yaml", 'r', encoding='utf-8'))
    STORAGE_MODE = config.get('storage_mode', 'local') # 전역적으로 모드 저장
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
class PipelineRequest(BaseModel): # 입력 값
    storage_mode: str = Field(
        default=STORAGE_MODE,
        description="스토리지 모드 (로컬 또는 S3)",
        example="local or s3"
    )
    query: str = Field(..., description="크롤링할 검색어 (필수)", example="성수동 카페")
    latitude: Optional[float] = Field(None, description="검색 기준점 위도 (선택)", example=37.544)
    longitude: Optional[float] = Field(None, description="검색 기준점 경도 (선택)", example=127.044)
    zoom_level: Optional[int] = Field(None, description="지도 확대 레벨(기본 값 15) (선택)", example=15) # [신규] zoom_level 필드 추가
    show_browser: bool = Field(False, description="크롤링 브라우저 창 표시 여부 (디버깅용)")

class TargetPipelineRequest(BaseModel): # 입력 값
    storage_mode: str = Field(
        default=STORAGE_MODE,
        description="스토리지 모드 (로컬 또는 S3)",
        example="local or s3"
    )
    # 가게 이름과 주소를 함께 받으면 더 정확한 검색이 가능합니다.
    query: str = Field(..., description="크롤링할 검색어 (필수)", example="성수동 카페")
    address: str = Field(..., description="크롤링할 가게의 정확한 주소(필수)", example="강남구")
    latitude: Optional[float] = Field(None, description="검색 기준점 위도 (선택)", example=37.544)
    longitude: Optional[float] = Field(None, description="검색 기준점 경도 (선택)", example=127.044)
    zoom_level: Optional[int] = Field(None, description="지도 확대 레벨(기본 값 15) (선택)", example=15) # [신규] zoom_level 필드 추가
    show_browser: bool = Field(False, description="크롤링 브라우저 창 표시 여부 (디버깅용)")

class TaskResponse(BaseModel): # 작업 응답 형식
    task_id: str
    message: str

class StatusResponse(BaseModel):
    task_id: str
    status: str
    request_details: Optional[Dict[str, Any]] = Field(None, description="최초 요청 파라미터")
    # ★★★ 상세 진행 상황을 위한 필드 추가
    progress: Optional[Dict[str, str]] = Field(None, description="파이프라인 단계별 진행 상황")
    result_path: Optional[str] = Field(None, description="[로컬 모드] 결과 파일이 저장된 로컬 경로")
    result_url: Optional[str] = Field(None, description="[S3 모드] 결과 파일 다운로드를 위한 임시 URL")
    error: Optional[str] = None
    error: Optional[str] = None

# --- 3. 핵심 파이프라인 실행 함수 ---
# 일반 파이프라인 실행 함수
def execute_pipeline_task(task_id: str, request: PipelineRequest, existing_ids: set):
    """오래 걸리는 전체 파이프라인 로직을 수행하는 함수 (백그라운드 실행용)"""
    output_dir = ""
    try:
        tasks_db[task_id] = {
            "status": "processing", # 전체 상태는 'processing'으로 유지
            "request_details": request.model_dump(),
            "progress": {
                "네이버 크롤링": "pending",
                "카카오 크롤링": "pending",
                "점수 산정": "pending",
                "결과 저장": "pending"
            }
        }
        print(f"[{task_id}] 파이프라인 시작: query='{request.query}'")
        
        # 1. Naver Crawling
        tasks_db[task_id]["progress"]["네이버 크롤링"] = "running"
        naver_df = run_naver_crawling(
            search_query=request.query,
            latitude=request.latitude,
            longitude=request.longitude,
            headless_mode=(not request.show_browser),
            zoom_level=request.zoom_level,
            existing_naver_ids=existing_ids
        )
        if naver_df.empty: raise ValueError("네이버 크롤링 결과가 없습니다.")
        tasks_db[task_id]["progress"]["네이버 크롤링"] = "completed"
        print(f"[{task_id}] 네이버 크롤링 완료.")

        # 2. Kakao Crawling
        tasks_db[task_id]["progress"]["카카오 크롤링"] = "running"
        kakao_df = run_kakao_crawling(
            input_df=naver_df,
            max_threads=config.get('num_threads', 3),
            headless=(not request.show_browser)
        )
        tasks_db[task_id]["progress"]["카카오 크롤링"] = "completed"
        print(f"[{task_id}] 카카오 크롤링 완료.")

        # 3. Scoring
        tasks_db[task_id]["progress"]["점수 산정"] = "running"
        final_list = run_scoring_pipeline(
            input_data=kakao_df.to_dict('records'),
            data_dir=config.get('data_dir', 'data')
        )
        if not final_list: raise ValueError("점수 산정 결과가 없습니다.")
        tasks_db[task_id]["progress"]["점수 산정"] = "completed"
        print(f"[{task_id}] 점수 산정 완료.")

        final_df = pd.DataFrame(final_list)
        
        # --- 공통 파일명 및 경로 생성 ---
        now = datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        safe_query_name = re.sub(r'[\\/*?:"<>|]', "", request.query)
        file_name = f"{safe_query_name}_{timestamp}_{task_id[:8]}_{len(final_df)}.json"
        
        tasks_db[task_id]["progress"]["결과 저장"] = "running" # 저장 시작

        # -- 스토리지 모드에 따른 결과 저장 분기 -- # 
        if request.storage_mode == "s3":
            s3_config = config['s3_config']
            s3_client = boto3.client('s3')
            date_path = now.strftime('%Y-%m/%Y-%m-%d')
            final_s3_key = f"{s3_config['output_results_prefix']}{date_path}/{file_name}"

            json_buffer = final_df.to_json(orient='records', force_ascii=False, indent=4)
            s3_client.put_object(Bucket=s3_config['bucket_name'], Key=final_s3_key, Body=json_buffer, ContentType='application/json')
            
            tasks_db[task_id].update({"status": "completed", "s3_key": final_s3_key})
            print(f"[{task_id}] S3에 개별 결과 저장 완료: {final_s3_key}")

        # local 모드일 경우
        else:
            local_config = config['local_config']
            date_path = now.strftime(os.path.join('%Y-%m', '%Y-%m-%d'))
            output_path = os.path.join(local_config['output_dir'], date_path)
            os.makedirs(output_path, exist_ok=True)
            
            final_local_path = os.path.join(output_path, file_name)
            final_df.to_json(final_local_path, orient='records', force_ascii=False, indent=4)
            
            tasks_db[task_id].update({"status": "completed", "result_path": final_local_path})
            print(f"[{task_id}] 로컬에 개별 결과 저장 완료: {final_local_path}")

    except Exception as e:
        error_message = str(e)
        tasks_db[task_id].update({"status": "failed", "error": error_message})
        print(f"[{task_id}] 파이프라인 오류: {error_message}")
        if output_dir:
            error_log_path = os.path.join(output_dir, "error_log.txt")
            with open(error_log_path, 'w', encoding='utf-8') as f:
                f.write(f"Error: {error_message}\n\n")
                f.write(traceback.format_exc())

# 타겟 파이프라인 실행 함수
def execute_target_pipeline_task(task_id: str, request: TargetPipelineRequest, existing_ids: set):
    """단일 타겟 매장에 대한 파이프라인 로직을 수행하는 함수 (백그라운드 실행용)"""
    output_dir = ""
    try:
        tasks_db[task_id] = {
            "status": "processing", # 전체 상태는 'processing'으로 유지
            "request_details": request.model_dump(),
            "progress": {
                "네이버 크롤링": "pending",
                "카카오 크롤링": "pending",
                "점수 산정": "pending",
                "결과 저장": "pending"
            }
        }
        print(f"[{task_id}] 타겟 파이프라인 시작: search_query='{request.query}', address='{request.address}'")

        # 1. Target Naver Crawling
        tasks_db[task_id]["progress"]["타겟 네이버 크롤링"] = "running"
        # 새로운 타겟 크롤러 컨트롤러 함수를 호출합니다.
        naver_df = run_target_naver_crawling(
            search_query=request.query,
            address=request.address,
            latitude=request.latitude,
            longitude=request.longitude,
            headless_mode=(not request.show_browser),
            zoom_level=request.zoom_level,
            existing_naver_ids=existing_ids

        )
        if naver_df.empty: raise ValueError("타겟 네이버 크롤링 결과가 없습니다.")
        tasks_db[task_id]["progress"]["타겟 네이버 크롤링"] = "completed"
        print(f"[{task_id}] 타겟 네이버 크롤링 완료.")

        # 2. Kakao Crawling
        tasks_db[task_id]["progress"]["카카오 크롤링"] = "running"
        kakao_df = run_kakao_crawling(
            input_df=naver_df,
            max_threads=config.get('num_threads', 3),
            headless=(not request.show_browser)
        )
        tasks_db[task_id]["progress"]["카카오 크롤링"] = "completed"
        print(f"[{task_id}] 카카오 크롤링 완료.")

        # 3. Scoring
        tasks_db[task_id]["progress"]["점수 산정"] = "running"
        final_list = run_scoring_pipeline(
            input_data=kakao_df.to_dict('records'),
            data_dir=config.get('data_dir', 'data')
        )
        if not final_list: raise ValueError("점수 산정 결과가 없습니다.")
        tasks_db[task_id]["progress"]["점수 산정"] = "completed"
        print(f"[{task_id}] 점수 산정 완료.")

        final_df = pd.DataFrame(final_list)
        
        # --- 공통 파일명 및 경로 생성 ---
        now = datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        safe_query_name = re.sub(r'[\\/*?:"<>|]', "", request.query)
        file_name = f"{safe_query_name}_{timestamp}_{task_id[:8]}_{len(final_df)}.json"
        
        tasks_db[task_id]["progress"]["결과 저장"] = "running" # 저장 시작

        # -- 스토리지 모드에 따른 결과 저장 분기 -- # 
        if request.storage_mode == "s3":
            s3_config = config['s3_config']
            s3_client = boto3.client('s3')
            date_path = now.strftime('%Y-%m/%Y-%m-%d')
            final_s3_key = f"{s3_config['output_results_prefix']}{date_path}/{file_name}"

            json_buffer = final_df.to_json(orient='records', force_ascii=False, indent=4)
            s3_client.put_object(Bucket=s3_config['bucket_name'], Key=final_s3_key, Body=json_buffer, ContentType='application/json')
            
            tasks_db[task_id].update({"status": "completed", "s3_key": final_s3_key})
            print(f"[{task_id}] S3에 개별 결과 저장 완료: {final_s3_key}")

        # local 모드일 경우
        else:
            local_config = config['local_config']
            date_path = now.strftime(os.path.join('%Y-%m', '%Y-%m-%d'))
            output_path = os.path.join(local_config['output_dir'], date_path)
            os.makedirs(output_path, exist_ok=True)
            
            final_local_path = os.path.join(output_path, file_name)
            final_df.to_json(final_local_path, orient='records', force_ascii=False, indent=4)
            
            tasks_db[task_id].update({"status": "completed", "result_path": final_local_path})
            print(f"[{task_id}] 로컬에 개별 결과 저장 완료: {final_local_path}")

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
    print(f"🚀 API 서버 시작... (스토리지 모드: {STORAGE_MODE.upper()})")
    setup_api_key()
    clean_firefox_cache()

# --- 5. API 엔드포인트 구현 --- # 이거 어떻게 post 넘겨서 값 받을 지 다시 정하기
# 일반 파이프라인 실행 함수 ------------------------------
@app.post("/pipeline/run", response_model=TaskResponse, status_code=202)
async def start_pipeline_endpoint(request: PipelineRequest, background_tasks: BackgroundTasks):
    """파이프라인 실행을 요청하고 즉시 작업 ID를 반환합니다."""
    task_id = str(uuid.uuid4())
    tasks_db[task_id] = {"status": "pending"}

    print(f"[{task_id}] 기존 마스터 데이터에서 naver_id 목록을 로드합니다.")
    existing_ids = load_ids_from_master_data(request.storage_mode, config)

    background_tasks.add_task(execute_pipeline_task, task_id, request, existing_ids)

    return {"task_id": task_id, "message": "파이프라인 작업이 접수되었습니다. '/pipeline/status/{task_id}'로 상태를 확인하세요."}

# 타겟 파이프라인 실행 함수 ------------------------------
@app.post("/pipeline/target-run", response_model=TaskResponse, status_code=202)
async def start_target_pipeline_endpoint(request: TargetPipelineRequest, background_tasks: BackgroundTasks):
    """(신규) 주소 정보를 활용한 단일 타겟 수집 파이프라인 실행을 요청합니다."""
    task_id = str(uuid.uuid4())
    tasks_db[task_id] = {"status": "pending"}
    
    # 타겟 크롤링은 보통 중복 ID 체크가 덜 중요하지만, 호환성을 위해 전달
    existing_ids = load_ids_from_master_data(request.storage_mode, config)
    
    # 타겟 파이프라인 실행 함수를 백그라운드 작업으로 추가
    background_tasks.add_task(execute_target_pipeline_task, task_id, request, existing_ids)

    return {"task_id": task_id, "message": "타겟 파이프라인 작업이 접수되었습니다. '/pipeline/status/{task_id}'로 상태를 확인하세요."}

# 파이프라인 상태 확인 함수 ------------------------------
@app.get("/pipelines/status/{task_id}", response_model=StatusResponse)
async def get_pipeline_status(task_id: str):
    """주어진 task_id에 대한 작업 상태와 결과 경로/URL을 반환합니다."""
    task = tasks_db.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="해당 task_id를 찾을 수 없습니다.")

    response_data = deepcopy(task) # 원본 DB 수정을 방지하기 위해 깊은 복사
    response_data['task_id'] = task_id

    # S3 모드이고 작업이 완료되었을 경우, 임시 URL(Presigned URL) 생성
    if STORAGE_MODE == 's3' and task.get("status") == "completed" and "s3_key" in task:
        s3_config = config['s3_config']
        s3_client = boto3.client('s3')
        try:
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': s3_config['bucket_name'], 'Key': task['s3_key']},
                ExpiresIn=3600  # 1시간 동안 유효한 URL
            )
            response_data['result_url'] = url
        except Exception as e:
            response_data['error'] = f"S3 URL 생성 실패: {e}"

    return response_data

# config 확인 함수 ------------------------------
@app.get("/config", response_model=dict)
async def get_config():
    """서버에 로드된 전체 설정을 확인합니다."""
    # 민감 정보를 포함할 수 있으므로 주의해서 사용해야 합니다.
    return config


# --- 6. 데이터 통합 수동 실행 API ---
def consolidation_task_wrapper():
    """배치 작업 실행 후 잠금 플래그를 해제하는 래퍼 함수."""
    global CONSOLIDATION_IN_PROGRESS
    try:
        run_consolidation_job()
    finally:
        CONSOLIDATION_IN_PROGRESS = False
        print("데이터 통합 작업 완료. 이제 다음 통합 요청을 받을 수 있습니다.")

# 데이터 통합 수동 실행 API ------------------------------
@app.post("/admin/consolidation", response_model=TaskResponse, status_code=202)
async def trigger_consolidation_endpoint(background_tasks: BackgroundTasks):
    """데이터 통합 배치 작업을 수동으로 실행시킵니다. (관리자용)"""
    #  함수 내에 'global' 키워드 선언 추가 
    global CONSOLIDATION_IN_PROGRESS
    
    if CONSOLIDATION_IN_PROGRESS:
        raise HTTPException(
            status_code=409,
            detail="데이터 통합 작업이 이미 실행 중입니다. 잠시 후 다시 시도해주세요."
        )

    CONSOLIDATION_IN_PROGRESS = True
    print("관리자 요청으로 데이터 통합 작업을 백그라운드에서 시작합니다.")
    background_tasks.add_task(consolidation_task_wrapper)
    
    return {"task_id": "consolidation_job", "message": "데이터 통합 작업이 시작되었습니다."}
