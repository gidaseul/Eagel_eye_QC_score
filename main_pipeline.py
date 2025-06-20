# main_pipeline.py

import os
import sys
import argparse
import yaml
import pandas as pd
from datetime import datetime
import ast
import re
import boto3
from typing import Set, Dict, List, Any

# .env 파일 로드를 위해 python-dotenv 설치 필요 (pip install python-dotenv)
from dotenv import load_dotenv
import google.generativeai as genai

# 각 단계별로 리팩토링된 모듈의 메인 함수를 import
from Crawling.naver_crawler import run_naver_crawling
from Crawling.kakao_crawler import run_kakao_crawling
from QC_score.score_pipline import run_scoring_pipeline
from Crawling.utils.master_loader import load_ids_from_master_data

CONFIG_ENV_PATH = ".config.env"
# .env 파일에서 환경 변수를 로드합니다.
load_dotenv(dotenv_path=CONFIG_ENV_PATH)

def load_config(config_path: str) -> dict:
    """YAML 설정 파일을 불러옵니다."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ 경고: 설정 파일 '{config_path}'를 찾을 수 없습니다. 코드의 기본값으로 실행합니다.")
        return {}
    except Exception as e:
        print(f"❌ 오류: 설정 파일 로딩 실패: {e}")
        return {}

def setup_api_key():
    """
    .env 파일에서 API 키를 로드하고 유효성을 검증합니다.
    파이프라인 시작 시 한 번만 실행됩니다.
    """
    load_dotenv()
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("오류: GOOGLE_API_KEY 환경 변수를 찾을 수 없습니다. .env 파일 또는 시스템 환경 변수를 확인하세요.", file=sys.stderr)
        return False
    
    try:
        genai.configure(api_key=api_key)
        list(genai.list_models()) # 간단한 API 호출로 키 유효성 검증
        print("✅ Google Gemini API 키가 성공적으로 검증되었습니다.")
        return True
    except Exception as e:
        print(f"❌ 오류: Gemini API 키가 유효하지 않습니다. 상세 오류: {e}", file=sys.stderr)
        return False

def ensure_list_or_dict(x):
    """문자열을 리스트나 딕셔너리로 안전하게 변환합니다."""
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        try:
            val = ast.literal_eval(x)
            if isinstance(val, (list, dict)):
                return val
        except (ValueError, SyntaxError):
            pass
    return x

def save_data(df: pd.DataFrame, filename_base: str, mode: str):
    """데이터프레임을 지정된 형식으로 저장합니다. 파일 확장자는 자동으로 붙습니다."""
    if df.empty:
        print(f"경고: 저장할 데이터가 없어 '{filename_base}' 파일 생성을 건너뜁니다.")
        return

    df = df.copy()
    for col in ['menu_list', 'review_info', 'theme_mood', 'theme_topic', 'theme_purpose', 'review_category']:
        if col in df.columns:
            df[col] = df[col].apply(ensure_list_or_dict)
            
    try:
        if mode in ["csv", "both"]:
            df.to_csv(f"{filename_base}.csv", encoding="utf-8-sig", index=False)
        if mode in ["json", "both"]:
            df.to_json(f"{filename_base}.json", orient='records', force_ascii=False, indent=4)
    except Exception as e:
        print(f"❌ 오류: 파일 저장 중 오류 발생 - {filename_base}. 에러: {e}", file=sys.stderr)

def main():
    """
    단일 검색 작업에 대한 전체 데이터 처리 파이프라인을 조율하고 실행합니다.
    """
    # --- 1. 설정 및 CLI 인자 처리 ---
    parser = argparse.ArgumentParser(description="Naver/Kakao Crawling and Scoring Pipeline for a single query.")
    
    # [수정] 입력 방식을 CSV에서 직접 검색어와 좌표로 변경
    parser.add_argument('-q', '--query', type=str, required=True, help='크롤링할 검색어 (필수)')
    parser.add_argument('--lat', type=float, help='검색 기준점 위도 (선택)')
    parser.add_argument('--lon', type=float, help='검색 기준점 경도 (선택)')

    # [유지] 기타 실행 옵션들
    parser.add_argument('--config', default='config.yaml', help='사용할 설정 파일의 경로')
    parser.add_argument('--stage', type=str, help="실행할 파이프라인 단계 ('naver', 'kakao', 'full')")
    parser.add_argument('--threads', type=int, help='카카오 크롤링에 사용할 스레드 개수')
    parser.add_argument('--show-browser', action='store_true', help='이 플래그 설정 시 크롤링 브라우저 창을 표시합니다.')
    parser.add_argument('--format', type=str, choices=['csv', 'json', 'both'], help="최종 결과 파일 저장 형식")
    
    args = parser.parse_args()
    config = load_config(args.config)

    # 설정값 결정 (우선순위: CLI > config.yaml > 기본값)
    PIPELINE_STAGE = args.stage or config.get('pipeline_stage', 'full')
    HEADLESS_MODE = not args.show_browser
    OUTPUT_FORMAT = args.format or config.get('output_format', 'both')
    DATA_DIR = config.get('data_dir', 'data')
    KAKAO_MAX_THREADS = args.threads or config.get('num_threads', 3)
    
    # 결과 저장을 위한 디렉토리 설정
    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    # 검색어를 파일 이름에 사용하기 위해 안전한 문자로 변환
    safe_query_name = re.sub(r'[\\/*?:"<>|]', "", args.query)
    OUTPUT_DIR = os.path.join(config.get('output_dir', 'results'), f"{safe_query_name}_{run_timestamp}")

    # --- 2. 초기 설정 ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not setup_api_key():
        sys.exit(1)

    # --- 3. 파이프라인 단계별 실행 ---
    print(f"\n===== 파이프라인 시작 (단계: {PIPELINE_STAGE.upper()}, 검색어: '{args.query}') =====")
    
    # ★★★ 수정된 부분: S3 로직을 로컬 JSON 로더 호출로 변경 ★★★
    # config.yaml에서 JSON 파일 경로를 읽어옵니다.
    id_json_path = config.get('id_json_path') # 예: 'data/existing_naver_ids.json'
    
    # 파이프라인 시작 시, 로컬 JSON 파일에서 ID 목록을 불러옵니다.
    if id_json_path:
        crawled_naver_ids = load_ids_from_master_data(id_json_path)
    else:
        print("경고: ID 목록 JSON 파일 경로(id_json_path)가 설정되지 않아, 세션 내 중복만 확인합니다.")
        crawled_naver_ids = set()
    
    
    current_df = pd.DataFrame()

    try:
        # [ 단계 1: 네이버 크롤링 ]
        if PIPELINE_STAGE in ['naver', 'kakao', 'full']:
            print(f"\n🚀 [STAGE: NAVER] 네이버 지도 크롤링을 시작합니다...")
            
            # [수정] 새로워진 run_naver_crawling 함수 호출
            current_df = run_naver_crawling(
                search_query=args.query,
                latitude=args.lat,
                longitude=args.lon,
                headless_mode=HEADLESS_MODE,
                output_dir=OUTPUT_DIR,
                existing_naver_ids=crawled_naver_ids
            )
            
            if current_df.empty:
                print("❌ 네이버 크롤링 결과가 없어 파이프라인을 중단합니다."); return

            # 새로 수집된 ID들을 전체 목록에 추가
            crawled_naver_ids.update(set(current_df['naver_id'].dropna().unique()))
            print(f"✅ 네이버 크롤링 완료. 고유 가게 {len(crawled_naver_ids)}개 수집.")
            
            save_data(current_df, os.path.join(OUTPUT_DIR, "1_naver_crawled"), OUTPUT_FORMAT)
            
            if PIPELINE_STAGE == 'naver':
                print("\n🎉 'naver' 단계 실행이 완료되었습니다."); return

        # [ 단계 2: 카카오 크롤링 ]
        if PIPELINE_STAGE in ['kakao', 'full']:
            print(f"\n🚀 [STAGE: KAKAO] 카카오맵 크롤링을 시작합니다...")
            current_df = run_kakao_crawling(input_df=current_df, max_threads=KAKAO_MAX_THREADS, headless=HEADLESS_MODE)
            save_data(current_df, os.path.join(OUTPUT_DIR, "2_kakao_added"), OUTPUT_FORMAT)
            print(f"✅ 카카오 크롤링 완료.")
            
            if PIPELINE_STAGE == 'kakao':
                print("\n🎉 'kakao' 단계까지의 실행이 완료되었습니다."); return

        # [ 단계 3: 점수 산정 ]
        if PIPELINE_STAGE == 'full':
            print(f"\n🚀 [STAGE: SCORING] 점수 산정을 시작합니다...")
            final_data_list = run_scoring_pipeline(input_data=current_df.to_dict('records'), data_dir=DATA_DIR)
            
            if not final_data_list:
                print("❌ 점수 산정 실패. 최종 파일을 저장하지 않고 파이프라인을 중단합니다."); return
            
            final_df = pd.DataFrame(final_data_list)
            final_output_base = os.path.join(OUTPUT_DIR, "3_final_scored_data")
            save_data(final_df, final_output_base, OUTPUT_FORMAT)
            
            print(f"✅ 점수 산정 완료. 최종 결과가 '{OUTPUT_DIR}' 폴더에 저장되었습니다.")
        
        print("\n🎉 전체 파이프라인 실행이 성공적으로 완료되었습니다!")

    except Exception as e:
        print(f"\n❌ 파이프라인 실행 중 오류가 발생했습니다: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()

