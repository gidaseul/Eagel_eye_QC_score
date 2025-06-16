# main_pipeline.py

import os
import sys
import argparse
import yaml
import pandas as pd
from datetime import datetime
import ast

# .env 파일 로드를 위해 python-dotenv 설치 필요 (pip install python-dotenv)
from dotenv import load_dotenv
import google.generativeai as genai

# 각 단계별로 리팩토링된 모듈의 메인 함수를 import
from Crawling.naver_crawler import run_naver_crawling
from Crawling.kakao_crawler import run_kakao_crawling
from QC_score.score_pipline import run_scoring_pipeline
CONFIG_ENV_PATH = ".config.env"
# .env 파일에서 환경 변수를 로드합니다.
load_dotenv(dotenv_path=CONFIG_ENV_PATH)

def load_config(config_path: str) -> dict:
    """YAML 설정 파일을 불러옵니다."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"경고: 설정 파일 '{config_path}'를 찾을 수 없습니다. 코드의 기본값으로 실행합니다.")
        return {}
    except Exception as e:
        print(f"오류: 설정 파일 로딩 실패: {e}")
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
        print(f"오류: Gemini API 키가 유효하지 않습니다. 상세 오류: {e}", file=sys.stderr)
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
        print(f"오류: 파일 저장 중 오류 발생 - {filename_base}. 에러: {e}", file=sys.stderr)

def main():
    """
    전체 데이터 처리 파이프라인을 조율하고 실행합니다.
    설정 우선순위: CLI 인자 > 설정 파일 (config.yaml) > 코드 내 기본값
    """
    # --- 1. 설정 및 CLI 인자 처리 ---
    parser = argparse.ArgumentParser(description="Naver/Kakao Crawling and Scoring Pipeline")
    parser.add_argument('--config', default='config.yaml', help='사용할 설정 파일의 경로')
    parser.add_argument('--stage', type=str, help="실행할 파이프라인 단계 ('naver', 'kakao', 'full')")
    parser.add_argument('--threads', type=int, help='실행할 스레드 개수 (1-3)')
    parser.add_argument('--show-browser', action='store_true', help='이 플래그 설정 시 크롤링 브라우저 창을 표시합니다.')
    parser.add_argument('--input-file', type=str, help="입력 CSV 파일 이름 (data_dir 내 위치해야 함)")
    parser.add_argument('--format', type=str, choices=['csv', 'json', 'both'], help="최종 결과 파일 저장 형식")

    
    args = parser.parse_args()
    config = load_config(args.config)

    # 설정값 결정 (우선순위: CLI > config.yaml > 기본값)
    # --- 설정값 결정 (우선순위: CLI > config.yaml > 코드 내 기본값) ---
    # 1. 먼저 설정 파일 또는 코드의 기본값을 설정
    PIPELINE_STAGE = config.get('pipeline_stage', 'full')
    NUM_THREADS = config.get('num_threads', 3)
    HEADLESS_MODE = config.get('headless_mode', True)
    INPUT_CSV_NAME = config.get('input_csv_name', 'input_data.csv')
    OUTPUT_FORMAT = config.get('output_format', 'both')
    SAVE_INTERVAL = config.get('save_interval', 100)
    DATA_DIR = config.get('data_dir', 'data')
    OUTPUT_DIR = config.get('output_dir', 'results')
    
    # 2. 만약 CLI 인자가 주어졌다면, 그 값으로 덮어쓰기
    if args.stage: PIPELINE_STAGE = args.stage
    if args.threads: NUM_THREADS = args.threads
    if args.show_browser: HEADLESS_MODE = False
    if args.input_file: INPUT_CSV_NAME = args.input_file
    if args.format: OUTPUT_FORMAT = args.format
    INPUT_CSV_PATH = os.path.join(DATA_DIR, INPUT_CSV_NAME)

    # --- 2. 초기 설정 및 유효성 검사 ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    today = datetime.now().strftime('%Y%m%d')

    if not setup_api_key():
        sys.exit(1)

    if not 1 <= NUM_THREADS <= 3:
        print(f"경고: 스레드 개수는 1에서 3 사이여야 합니다. {NUM_THREADS} -> 3으로 조정합니다.")
        NUM_THREADS = 3

    # --- 3. 파이프라인 단계별 실행 ---
    print(f"\n===== 파이프라인 시작 (단계: {PIPELINE_STAGE.upper()}) =====")
    try:
        # [ 단계 1: 네이버 크롤링 ]
        if PIPELINE_STAGE in ['naver', 'kakao', 'full']:
            print(f"\n🚀 [STAGE: NAVER] 네이버 지도 크롤링을 시작합니다...")
            effective_save_interval = SAVE_INTERVAL if NUM_THREADS > 1 else 0
            current_df = run_naver_crawling(csv_path=INPUT_CSV_PATH, num_threads=NUM_THREADS, headless_mode=HEADLESS_MODE, save_interval=effective_save_interval, output_dir=OUTPUT_DIR)
            if current_df.empty:
                print("❌ 네이버 크롤링 결과가 없어 파이프라인을 중단합니다."); return
            save_data(current_df, os.path.join(OUTPUT_DIR, f"1_naver_crawled_{today}"), 'csv')
            print(f"✅ 네이버 크롤링 완료. 중간 결과 저장 완료.")
            if PIPELINE_STAGE == 'naver':
                print("\n🎉 'naver' 단계 실행이 완료되었습니다."); return

        # [ 단계 2: 카카오 크롤링 ]
        if PIPELINE_STAGE in ['kakao', 'full']:
            print(f"\n🚀 [STAGE: KAKAO] 카카오맵 크롤링을 시작합니다...")
            current_df = run_kakao_crawling(input_df=current_df, max_threads=NUM_THREADS, headless=HEADLESS_MODE)
            save_data(current_df, os.path.join(OUTPUT_DIR, f"2_kakao_added_{today}"), 'csv')
            print(f"✅ 카카오 크롤링 완료. 중간 결과 저장 완료.")
            if PIPELINE_STAGE == 'kakao':
                print("\n🎉 'kakao' 단계까지의 실행이 완료되었습니다."); return

        # [ 단계 3: 점수 산정 ]
        if PIPELINE_STAGE == 'full':
            print(f"\n🚀 [STAGE: SCORING] 점수 산정을 시작합니다...")
            final_data_list = run_scoring_pipeline(input_data=current_df.to_dict('records'), data_dir=DATA_DIR)
            if not final_data_list or 'Total_점수' not in final_data_list[0]:
                print("❌ 점수 산정 실패. 최종 파일을 저장하지 않고 파이프라인을 중단합니다."); return
            
            final_df = pd.DataFrame(final_data_list)
            final_output_base = os.path.join(OUTPUT_DIR, f"3_final_scored_data_{today}")
            save_data(final_df, final_output_base, OUTPUT_FORMAT)
            
            saved_files = [f"{final_output_base}.{ext}" for ext in (['csv', 'json'] if OUTPUT_FORMAT == 'both' else [OUTPUT_FORMAT])]
            print(f"✅ 점수 산정 완료. 최종 결과 저장: {', '.join(saved_files)}")
        
        print("\n🎉 전체 파이프라인 실행이 성공적으로 완료되었습니다!")


    except Exception as e:
        print(f"\n💥 파이프라인 실행 중 오류가 발생했습니다: {e}", file=sys.stderr)
        # 상세 오류 출력을 위해 traceback import
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()