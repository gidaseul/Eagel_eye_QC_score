# naver_crawler.py

import pandas as pd
import logging
import os
import sys
import ast
from typing import Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# naver_crawler_detail에서 StoreCrawler 클래스를 import
# 사용자의 경로에 맞게 수정: from Crawling.naver_crawer_detail import StoreCrawler
# 만약 naver_crawler.py와 naver_crawler_detail.py가 같은 폴더에 있다면 아래와 같이 수정합니다.
from .naver_crawler_detail import StoreCrawler

# --- 유틸리티 함수 ---


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

def run_naver_crawling(
    search_query: str,
    latitude: float = None,
    longitude: float = None,
    zoom_level: Optional[int] = None, # [신규] zoom_level 인자 추가
    headless_mode: bool = True,
    output_dir: str = 'results',
    existing_naver_ids: set = None 

) -> pd.DataFrame:
    """
    단일 검색 작업에 대한 네이버 지도 크롤링을 실행하고, 결과를 DataFrame으로 반환합니다.

    Args:
        search_query (str): 검색할 키워드.
        latitude (float, optional): 검색 기준점 위도. Defaults to None.
        longitude (float, optional): 검색 기준점 경도. Defaults to None.
        headless_mode (bool, optional): 브라우저 창 숨김 여부. Defaults to True.
        output_dir (str, optional): 결과물이 저장될 디렉토리. Defaults to 'results'.

    Returns:
        pd.DataFrame: 크롤링 결과를 통합한 데이터프레임.
    """
    print(f"네이버 크롤링 시작... (검색어: '{search_query}')")
    # 1. StoreCrawler 인스턴스 생성
    crawler = StoreCrawler(headless=headless_mode, output_base_dir=output_dir,existing_naver_ids=existing_naver_ids)
    
    # WebDriver가 성공적으로 초기화되었는지 확인
    if crawler.driver is None:
        print("WebDriver 초기화에 실패하여 크롤링을 중단합니다.")
        return pd.DataFrame()

    # 2. 크롤링 실행
    final_df = crawler.run_crawl(
        search_query=search_query,
        latitude=latitude,
        longitude=longitude,
        zoom_level=zoom_level
    )
    
    # 3. 중복 제거 (naver_id 기준)
    if not final_df.empty and 'naver_id' in final_df.columns:
        final_df.drop_duplicates(subset=["naver_id"], inplace=True, keep='first')
        print(f"총 {len(final_df)}개의 고유한 매장 정보 크롤링을 완료했습니다.")
    else:
        print("크롤링된 데이터가 없습니다.")

    return final_df


# 이 파일을 직접 실행할 경우에만 아래 코드가 동작합니다.
# [수정] 파일을 직접 실행할 경우의 테스트 로직 변경
if __name__ == "__main__":
    print("--- Naver Crawler 단독 실행 모드 ---")

    # 사용자 입력 받기
    search_query_input = input("검색어를 입력하세요 (예: 강남역 맛집): ").strip()
    if not search_query_input:
        print("검색어는 필수입니다. 프로그램을 종료합니다.")
        exit()

    coords_input = input("좌표를 입력하시겠습니까? (Y/N) [기본: N]: ").strip().upper()
    lat_input, lon_input = None, None
    if coords_input == 'Y':
        try:
            lat_input = float(input("위도(latitude)를 입력하세요: ").strip())
            lon_input = float(input("경도(longitude)를 입력하세요: ").strip())
        except ValueError:
            print("숫자 형식의 좌표를 입력해야 합니다. 좌표 없이 진행합니다.")
            lat_input, lon_input = None, None

    headless_input_str = (input("브라우저 창을 숨길까요? (Y/N) [기본: Y]: ").strip().upper() or "Y")
    headless_input = (headless_input_str == "Y")
    
    # 결과 저장 디렉토리 설정
    run_date = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_directory = os.path.join(os.path.dirname(__file__), "results", run_date)
    os.makedirs(output_directory, exist_ok=True)

    # 정의된 함수 호출
    final_dataframe = run_naver_crawling(
        search_query=search_query_input,
        latitude=lat_input,
        longitude=lon_input,
        headless_mode=headless_input,
        output_dir=output_directory
    )

    # 최종 결과 파일로 저장
    if not final_dataframe.empty:
        # naver_crawler.py에 save_data가 없으므로 간단히 to_csv/to_json으로 저장
        final_path_base = os.path.join(output_directory, f"crawled_data_{run_date}")
        final_dataframe.to_csv(f"{final_path_base}.csv", index=False, encoding='utf-8-sig')
        final_dataframe.to_json(f"{final_path_base}.json", orient='records', force_ascii=False, indent=2)
        print(f"\n최종 결과가 저장되었습니다: {final_path_base}.csv/.json")
    else:
        print("\n최종 결과가 비어있어 파일을 저장하지 않았습니다.")

