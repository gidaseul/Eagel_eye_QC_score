# naver_crawler.py

import pandas as pd
import logging
import os
import sys
import ast
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# naver_crawler_detail에서 StoreCrawler 클래스를 import
# 사용자의 경로에 맞게 수정: from Crawling.naver_crawer_detail import StoreCrawler
# 만약 naver_crawler.py와 naver_crawler_detail.py가 같은 폴더에 있다면 아래와 같이 수정합니다.
from .naver_crawler_detail import StoreCrawler

# --- 유틸리티 함수 ---

def read_store_info(csv_path: str) -> list:
    """CSV 파일에서 가게 이름과 위치 정보를 읽어 리스트로 반환합니다."""
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        return [{"name": row["name"], "location": row["location"]} for _, row in df.iterrows()]
    except FileNotFoundError:
        print(f"오류: 입력 파일 '{csv_path}'를 찾을 수 없습니다.")
        return []

def split_store_list(stores: list, num_threads: int) -> list:
    """가게 목록을 스레드 개수에 맞게 분배합니다."""
    if not stores or num_threads <= 0:
        return []
    if num_threads == 1:
        return [stores]
    
    chunk_size = len(stores) // num_threads
    chunks = [stores[i*chunk_size:(i+1)*chunk_size] for i in range(num_threads - 1)]
    chunks.append(stores[(num_threads - 1) * chunk_size:])
    return chunks

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

def save_data(df: pd.DataFrame, filename: str, mode: str):
    """데이터프레임을 지정된 형식으로 저장합니다."""
    df = df.copy()
    for col in ['menu_list', 'review_info', 'theme_mood', 'theme_topic', 'theme_purpose', 'review_category']:
        if col in df.columns:
            df[col] = df[col].apply(ensure_list_or_dict)
            
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x).strip().replace("\n", " ").replace("\r", " ") if pd.notna(x) else x)

    if mode in ["csv", "both"]:
        df.to_csv(f"{filename}.csv", encoding="utf-8-sig", index=False)
    if mode in ["json", "both"]:
        df.to_json(f"{filename}.json", orient='records', force_ascii=False, indent=4)


def crawl_store_group(stores: list, thread_id: int, save_mode: str, save_interval: int, headless_mode: bool, run_date_dir: str) -> pd.DataFrame:
    """
    한 스레드가 담당할 가게 목록을 크롤링하는 함수입니다.
    """
    if not stores:
        return pd.DataFrame()

    mid_buffer = []
    final_results = []
    save_seq = 0

    # 스레드 시작 시 크롤러 인스턴스를 한 번만 생성
    first_store = stores[0]
    crawler = StoreCrawler(
        location=first_store["location"],
        name=first_store["name"],
        output_base_dir=run_date_dir,
        save_threshold=save_interval, # 이 인자는 StoreCrawler 내부에서는 현재 사용되지 않음
        headless=headless_mode,
        thread_id=thread_id,
    )
    # WebDriver가 성공적으로 초기화되었는지 확인
    if crawler.driver is None:
        print(f"[Thread {thread_id}] WebDriver 초기화 실패. 스레드를 종료합니다.")
        return pd.DataFrame()

    for store in stores:
        try:
            # 매장별 속성 갱신 및 초기화
            crawler.origin_name = store["name"]
            crawler.location = store["location"]
            crawler.search_word = store["name"]
            
            # 메인 페이지로 이동하여 검색 상태 초기화
            crawler.go_to_main_page()

            if crawler.get_into_store():
                crawler.get_store_details() # 여기서 store_dict가 채워지고 self.data에 추가됨
                
                # StoreCrawler의 data에서 마지막 행을 가져옴
                if not crawler.data.empty:
                    row = crawler.data.iloc[[-1]].copy()
                    mid_buffer.append(row)
                    final_results.append(row)

                    # 중간 저장 로직
                    if save_interval > 0 and len(mid_buffer) >= save_interval:
                        save_seq += 1
                        mid_dir = os.path.join(run_date_dir, f"mid_save", f"thread_{thread_id}")
                        os.makedirs(mid_dir, exist_ok=True)
                        save_data(pd.concat(mid_buffer), os.path.join(mid_dir, f"mid_{save_seq}"), "json")
                        mid_buffer = []

        except Exception as e:
            crawler.logger.warning(f"[Thread {thread_id}] 매장 크롤링 중 오류 발생 - {store['name']}: {e}", exc_info=True)
            # 심각한 오류 시 드라이버 재시작 고려
            # crawler.restart_driver_inline() 
            continue

    # 남은 데이터 중간 저장
    if save_interval > 0 and mid_buffer:
        save_seq += 1
        mid_dir = os.path.join(run_date_dir, f"mid_save", f"thread_{thread_id}")
        os.makedirs(mid_dir, exist_ok=True)
        save_data(pd.concat(mid_buffer), os.path.join(mid_dir, f"mid_{save_seq}"), "json")

    # 스레드 드라이버 종료
    try:
        crawler.driver.quit()
    except Exception as e:
        crawler.logger.warning(f"WebDriver 종료 중 오류 발생: {e}")

    return pd.concat(final_results, ignore_index=True) if final_results else pd.DataFrame()


def run_naver_crawling(
    csv_path: str,
    num_threads: int,
    headless_mode: bool,
    save_interval: int,
    output_dir: str,
    save_mode: str = "json"
) -> pd.DataFrame:
    """
    네이버 지도 크롤링 파이프라인을 실행하고, 결과를 DataFrame으로 반환합니다.

    Args:
        csv_path (str): 크롤링할 매장 목록이 담긴 CSV 파일 경로.
        num_threads (int): 실행할 스레드 개수.
        headless_mode (bool): 브라우저 창 숨김 여부.
        save_interval (int): 스레드별 중간 저장 간격 (0이면 비활성화).
        output_dir (str): 결과물이 저장될 디렉토리.
        save_mode (str): 중간 저장 파일 형식 ('csv', 'json', 'both').

    Returns:
        pd.DataFrame: 모든 스레드의 크롤링 결과를 통합한 데이터프레임.
    """
    print(f"네이버 크롤링 시작... (입력: {csv_path}, 스레드: {num_threads}개)")
    stores = read_store_info(csv_path)
    if not stores:
        return pd.DataFrame()

    store_chunks = split_store_list(stores, num_threads)
    
    results_dfs = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {
            executor.submit(crawl_store_group, chunk, i, save_mode, save_interval, headless_mode, output_dir): i
            for i, chunk in enumerate(store_chunks)
        }
        
        for future in as_completed(futures):
            thread_id = futures[future]
            try:
                thread_df = future.result()
                if not thread_df.empty:
                    results_dfs.append(thread_df)
                    print(f"✅ [Thread {thread_id}] 작업 완료, {len(thread_df)}개 결과 반환.")
            except Exception as e:
                print(f"❌ [Thread {thread_id}] 실행 중 오류 발생: {e}", file=sys.stderr)

    if not results_dfs:
        print("모든 스레드에서 크롤링 결과가 없습니다.")
        return pd.DataFrame()

    final_df = pd.concat(results_dfs, ignore_index=True)
    final_df.drop_duplicates(subset=["naver_id"], inplace=True, keep='first')
    
    print(f"총 {len(final_df)}개의 고유한 매장 정보 크롤링을 완료했습니다.")
    
    return final_df


# 이 파일을 직접 실행할 경우에만 아래 코드가 동작합니다.
if __name__ == "__main__":
    print("--- Naver Crawler 단독 실행 모드 ---")
    
    default_csv_path = os.path.join(os.path.dirname(__file__), "name_location_output.csv")
    default_threads = 2
    default_save_interval = 100
    
    # 사용자 입력 받기
    csv_input = input(f"입력 CSV 파일 경로 [기본: {default_csv_path}]: ").strip() or default_csv_path
    num_threads_input = int(input(f"스레드 개수 [기본: {default_threads}]: ").strip() or default_threads)
    headless_input_str = (input("브라우저 창을 숨길까요? (Y/N) [기본: Y]: ").strip().upper() or "Y")
    headless_input = (headless_input_str == "Y")
    
    if num_threads_input > 1:
        save_interval_input = int(input(f"중간 저장 간격 (0이면 저장 안함) [기본: {default_save_interval}]: ").strip() or default_save_interval)
    else:
        print("스레드가 1개이므로 중간 저장을 비활성화합니다.")
        save_interval_input = 0

    # 결과 저장 디렉토리 설정
    run_date = datetime.now().strftime('%Y%m%d')
    output_directory = os.path.join(os.path.dirname(__file__), "results", run_date)
    os.makedirs(output_directory, exist_ok=True)

    # 정의된 함수 호출
    final_dataframe = run_naver_crawling(
        csv_path=csv_input,
        num_threads=num_threads_input,
        headless_mode=headless_input,
        save_interval=save_interval_input,
        output_dir=output_directory,
        save_mode="both"
    )
    
    # 최종 결과 파일로 저장
    if not final_dataframe.empty:
        final_path = os.path.join(output_directory, f"store_crawling_{run_date}_FINAL")
        save_data(final_dataframe, final_path, "both")
        print(f"\n✅ 최종 결과가 저장되었습니다: {final_path}.csv/.json")
    else:
        print("\n최종 결과가 비어있어 파일을 저장하지 않았습니다.")