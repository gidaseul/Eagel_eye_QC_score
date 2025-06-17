# main_pipeline.py

import os
import sys
import argparse
import yaml
import pandas as pd
from datetime import datetime

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
    
    args = parser.parse_args()
    config = load_config(args.config)

    # 설정값 결정 (우선순위: CLI > config.yaml > 기본값)
    PIPELINE_STAGE = args.stage or config.get('pipeline_stage', 'full')
    NUM_THREADS = args.threads or config.get('num_threads', 3)
    HEADLESS_MODE = not args.show_browser if args.show_browser else config.get('headless_mode', True)
    SAVE_INTERVAL = config.get('save_interval', 100)
    
    DATA_DIR = config.get('data_dir', 'data')
    OUTPUT_DIR = config.get('output_dir', 'results')
    INPUT_CSV_NAME = args.input_file or config.get('input_csv_name', 'name_location_output.csv')
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
        print(f"\n🚀 [STAGE: NAVER] 네이버 지도 크롤링을 시작합니다...")
        
        # 스레드 1개일 경우 중간 저장 비활성화
        effective_save_interval = SAVE_INTERVAL if NUM_THREADS > 1 else 0
        
        naver_df = run_naver_crawling(
            csv_path=INPUT_CSV_PATH,
            num_threads=NUM_THREADS,
            headless_mode=HEADLESS_MODE,
            save_interval=effective_save_interval,
            output_dir=OUTPUT_DIR,
        )

        if naver_df.empty:
            print("❌ 네이버 크롤링 결과가 없어 파이프라인을 중단합니다.")
            return

        # 네이버 크롤링 결과 저장 (중간 산출물)
        naver_output_path = os.path.join(OUTPUT_DIR, f"1_naver_crawled_{today}.csv")
        naver_df.to_csv(naver_output_path, index=False, encoding='utf-8-sig')
        print(f"✅ 네이버 크롤링 완료. 중간 결과 저장: {naver_output_path}")

        if PIPELINE_STAGE == 'naver':
            print("\n🎉 'naver' 단계 실행이 완료되었습니다.")
            return

        # [ 단계 2: 카카오 크롤링 ]
        print(f"\n🚀 [STAGE: KAKAO] 카카오맵 크롤링을 시작합니다...")
        kakao_df = run_kakao_crawling(
            input_df=naver_df,
            max_threads=NUM_THREADS,
            headless=HEADLESS_MODE
        )
        
        kakao_output_path = os.path.join(OUTPUT_DIR, f"2_kakao_added_{today}.csv")
        kakao_df.to_csv(kakao_output_path, index=False, encoding='utf-8-sig')
        print(f"✅ 카카오 크롤링 완료. 중간 결과 저장: {kakao_output_path}")

        if PIPELINE_STAGE == 'kakao':
            print("\n🎉 'kakao' 단계까지의 실행이 완료되었습니다.")
            return
            
        # [ 단계 3: 점수 산정 ]
        print(f"\n🚀 [STAGE: SCORING] 점수 산정을 시작합니다...")
        # 데이터프레임을 scoring 모듈이 요구하는 딕셔너리 리스트 형태로 변환
        final_data_list = run_scoring_pipeline(
            input_data=kakao_df.to_dict('records'),
            data_dir=DATA_DIR # 점수 산정용 데이터(json, csv)가 있는 폴더 경로 전달
        )
        final_df = pd.DataFrame(final_data_list)
        
        # 최종 결과물 저장
        final_output_path = os.path.join(OUTPUT_DIR, f"3_final_scored_data_{today}.csv")
        final_df.to_csv(final_output_path, index=False, encoding='utf-8-sig')
        print(f"✅ 점수 산정 완료. 최종 결과 저장: {final_output_path}")
        
        print("\n🎉 전체 파이프라인 실행이 성공적으로 완료되었습니다!")

    except Exception as e:
        print(f"\n💥 파이프라인 실행 중 오류가 발생했습니다: {e}", file=sys.stderr)
        # 상세 오류 출력을 위해 traceback import
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()