import json
from QC_score.score_pipline import run_scoring_pipeline
from main_pipeline import setup_api_key
from dotenv import load_dotenv
import google.generativeai as genai

import os
import sys
import argparse
import yaml
import pandas as pd
from datetime import datetime
import ast
import re

# 테스트를 위한 설정
TEST_INPUT_FILE = "/Users/10fingers/Desktop/Eagel_eye_final/results/낙지탕_20250619_101233/2_kakao_added.csv"
# 점수 산정에 필요한 데이터(매핑, 폴리곤 등)가 있는 폴더
DATA_DIR = "data"
# 최종 결과가 저장될 파일 이름
TEST_OUTPUT_FILE = "scored_test_output.json"


CONFIG_ENV_PATH = ".config.env"
# .env 파일에서 환경 변수를 로드합니다.
load_dotenv(dotenv_path=CONFIG_ENV_PATH)
# [신규] CSV의 문자열을 list/dict로 변환하기 위한 헬퍼 함수
def ensure_list_or_dict(x):
    if isinstance(x, (list, dict)):
        return x
    if pd.isna(x):
        return [] if '[' in str(x) else {} # 기본값 추론
    if isinstance(x, str):
        try:
            return ast.literal_eval(x)
        except (ValueError, SyntaxError):
            return x # 변환 실패 시 원본 반환
    return x


def setup_api_key():
    # ... (기존과 동일)
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key: return False
    try:
        genai.configure(api_key=api_key)
        list(genai.list_models())
        print("✅ Google Gemini API 키가 성공적으로 검증되었습니다.")
        return True
    except Exception as e:
        print(f"❌ 오류: Gemini API 키가 유효하지 않습니다: {e}")
        return False

def main():
    print("--- Score Pipeline 단독 테스트 시작 ---")

    if not setup_api_key():
        return

    try:
        df = pd.read_csv(TEST_INPUT_FILE)
        
        # [핵심 수정] CSV에서 문자열로 읽어온 컬럼들을 실제 list/dict 타입으로 변환
        # 이 과정이 누락되어 오류가 발생했습니다.
        list_like_columns = ['menu_list', 'review_info', 'theme_mood', 'theme_topic', 'theme_purpose', 'review_category']
        print("\n데이터 타입 변환 시작...")
        for col in list_like_columns:
            if col in df.columns:
                print(f"- '{col}' 컬럼 변환 중...")
                # NaN이 아닌 값에 대해서만 변환 함수 적용
                df[col] = df.loc[df[col].notna(), col].apply(ensure_list_or_dict)
        print("✅ 데이터 타입 변환 완료.")

        test_data = df.to_dict('records')
        print(f"✅ 테스트 데이터 준비 완료: {TEST_INPUT_FILE} ({len(test_data)}개 항목)")
    except Exception as e:
        print(f"❌ 테스트 데이터 준비 중 오류 발생: {e}")
        return

    print("\n🚀 스코어링 파이프라인을 실행합니다...")
    scored_data = run_scoring_pipeline(input_data=test_data, data_dir=DATA_DIR)

    # 4. 결과 확인 및 저장
    if scored_data:
        print(f"✅ 스코어링 완료. {len(scored_data)}개 항목 처리됨.")
        
        print("\n--- 첫 번째 항목 처리 결과 예시 ---")
        print(json.dumps(scored_data[0], indent=2, ensure_ascii=False))
        
        try:
            with open(TEST_OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(scored_data, f, indent=2, ensure_ascii=False)
            print(f"\n✅ 전체 결과가 '{TEST_OUTPUT_FILE}' 파일에 저장되었습니다.")
        except Exception as e:
            print(f"❌ 결과 파일 저장 실패: {e}")
    else:
        print("❌ 스코어링 결과가 없습니다.")

if __name__ == '__main__':
    main()
