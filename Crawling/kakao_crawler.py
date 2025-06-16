# kakao_parallel_crawler.py
import os
import sys
import json
import time
import re
import random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from Levenshtein import distance
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



# import Crawling_config as confing

# ============ 설정 =============
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12.6; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0",
]

# ============ KakaoMapCrawler 클래스 =============
class KakaoMapCrawler:
    def __init__(self, thread_id: int = 0, headless: bool = True):
        self.thread_id = thread_id
        self.user_agent = USER_AGENTS[thread_id % len(USER_AGENTS)]
        self.headless = headless
        self.driver = self._init_driver()
        self.wait = WebDriverWait(self.driver, 10) if self.driver else None
    
    def _init_driver(self):
        """WebDriver를 초기화합니다."""
        try:
            options = Options()
            if self.headless:
                options.add_argument("--headless")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--lang=ko-KR")
            options.add_argument(f"user-agent={self.user_agent}")
            
            # ▼▼▼ [수정] 하드코딩된 geckodriver 절대 경로 제거 ▼▼▼
            service = Service() # 시스템 PATH에 설치된 geckodriver를 자동으로 사용
            return webdriver.Firefox(options=options, service=service)
        except Exception as e:
            print(f"[Thread {self.thread_id}] WebDriver 초기화 실패: {e}", file=sys.stderr)
            return None

    def crawl_store(self, store: dict) -> dict:
        """단일 매장 정보를 크롤링합니다."""
        if not self.driver:
            return {**store, **self._empty_fields(prefix=True)}

        base_url = "https://map.kakao.com/"
        try:
            self.driver.get(base_url)
            # 'name' 키가 없을 경우를 대비하여 .get() 사용
            self._search(store.get("name", ""))
            time.sleep(1)

            if self._check_result_type() == "single":
                detail_url = self._get_single_result_url()
            else:
                candidates = self._get_multiple_results()
                best_match = self._match_address(store.get("address", ""), candidates)
                detail_url = best_match["url"] if best_match else None

            if not detail_url:
                return {**store, **self._empty_fields(prefix=True)}

            self.driver.get(detail_url)
            data = self._scrape_detail()
            
            # ▼▼▼ [수정] 결과 병합 로직 명확화 ▼▼▼
            result = {**store}
            for key, value in data.items():
                result[f'kakao_{key}'] = value
            return result
            
        except Exception as e:
            print(f"[Thread {self.thread_id}] 매장 '{store.get('name', 'N/A')}' 크롤링 중 오류: {e}", file=sys.stderr)
            return {**store, **self._empty_fields(prefix=True)}


    def _empty_fields(self, prefix=False):
        fields = {
            "score": None, "review": None, "taste": 0, "value": 0,
            "kindness": 0, "mood": 0, "parking": 0,
        }
        if prefix:
            return {f"kakao_{k}": v for k, v in fields.items()}
        return fields

    def _search(self, keyword):
        input_box = self.wait.until(EC.presence_of_element_located((By.ID, "search.keyword.query")))
        input_box.clear()
        input_box.send_keys(keyword)
        try:
            self.wait.until(EC.invisibility_of_element_located((By.ID, "dimmedLayer")))
        except:
            time.sleep(1)
        btn = self.wait.until(EC.element_to_be_clickable((By.ID, "search.keyword.submit")))
        self.driver.execute_script("arguments[0].click();", btn)
        self.wait.until(EC.presence_of_element_located((By.ID, "info.search.place.list")))

    def _check_result_type(self):
        try:
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "moreview")))
            return "single"
        except:
            return "multiple"

    def _get_single_result_url(self):
        return self.driver.find_element(By.CLASS_NAME, "moreview").get_attribute("href")

    def _get_multiple_results(self):
        items = self.driver.find_elements(By.CSS_SELECTOR, "ul.placelist li")
        results = []
        for item in items:
            try:
                name = item.find_element(By.CLASS_NAME, "link_name").text
                address = item.find_element(By.CSS_SELECTOR, "p[data-id='address']").get_attribute("title")
                url = item.find_element(By.CLASS_NAME, "moreview").get_attribute("href")
                results.append({"name": name, "address": address, "url": url})
            except:
                continue
        return results

    def _match_address(self, target, candidates):
        best, best_score = None, float("inf")
        for c in candidates:
            score = distance(target, c["address"])
            if score < best_score:
                best_score = score
                best = c
        return best

    def _scrape_detail(self):
        data = self._empty_fields()
        try:
            review_tab = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='#comment'].link_tab"))
            )
            self.driver.execute_script("arguments[0].click();", review_tab)
            self.wait.until(lambda d: review_tab.get_attribute("aria-selected") == "true")
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            # 별점 크롤랑
            score = self.driver.find_elements(By.CSS_SELECTOR, "div.group_total span.num_star")
            if score:
                data["score"] = float(score[0].text.strip())

            # 리뷰 크롤랑
            review = self.driver.find_elements(By.CSS_SELECTOR, "div.group_total strong.tit_total")
            if review:
                match = re.search(r"\d+", review[0].text.strip())
                if match:
                    data["review"] = int(match.group())

            # 더보기 버튼 클릭
            
            try:
                more_btn = self.driver.find_elements(By.CSS_SELECTOR, "div.area_more > button.btn_more")
                if more_btn and more_btn[0].get_attribute("aria-expanded") == "true":
                    self.driver.execute_script("arguments[0].click();", more_btn[0])
                    self.wait.until(lambda d: more_btn[0].get_attribute("aria-expanded") == "false")
            except:
                pass # 더 보기 없는 경우

            # 키워드 항목 수집 (맛, 분위기 등)
            items = self.driver.find_elements(By.CSS_SELECTOR, "div.wrap_point.open ul.list_point li")
            keyword_map = {
                "맛": "taste",
                "가성비": "value",
                "친절": "kindness",
                "분위기": "mood",
                "주차": "parking"
            }
            for item in items:
                try:
                    label = item.find_element(By.CSS_SELECTOR, "span.txt_point").text.strip()
                    count_text = item.find_element(By.CSS_SELECTOR, "span.rate_point").text.strip()
                    count = int(re.search(r"\d+", count_text).group())
                    if label in keyword_map:
                        data[keyword_map[label]] = count
                except:
                    continue

            # 키워드 모두 0이면 None 처리
            if data["score"] is not None and data["review"] is not None:
                keyword_keys = ["taste", "value", "kindness", "mood", "parking"]
                if all(data[k] == 0 for k in keyword_keys):
                    print(f"[Thread {self.thread_id}] Warning: score/review 존재하나 키워드가 모두 0 → None 처리")
                    for k in keyword_keys:
                        data[k] = None

        except Exception as e:
            print(f"[Thread {self.thread_id}] Detail scrape error: {e}")
        return data

    def _save_intermediate(self):
        df = pd.DataFrame(self.partial_results)
        df.to_csv(f"logs/thread_{self.thread_id}_part{self.part_count}.csv", index=False, encoding="utf-8-sig")
        df.to_json(f"logs/thread_{self.thread_id}_part{self.part_count}.json", orient="records", force_ascii=False, indent=2)
        print(f"[Thread {self.thread_id}] 중간 저장 완료 (part {self.part_count})")
        self.part_count += 1
        self.partial_results = []

    def quit(self):
        """드라이버를 종료합니다."""
        if self.driver:
            self.driver.quit()

# --- 스레드 작업 단위 함수 ---
def crawl_one(store: dict, thread_id: int, headless: bool) -> dict:
    crawler = KakaoMapCrawler(thread_id=thread_id, headless=headless)
    result = crawler.crawl_store(store)
    crawler.quit()
    return result

def run_kakao_crawling(input_df: pd.DataFrame, max_threads: int, headless: bool) -> pd.DataFrame:
    """
    입력받은 DataFrame에 대해 카카오맵 정보를 병렬로 크롤링하여 추가하고 결과를 반환합니다.

    Args:
        input_df (pd.DataFrame): 네이버 크롤링 결과가 담긴 데이터프레임.
        max_threads (int): 실행할 최대 스레드 개수.
        headless (bool): 브라우저 창 숨김 여부.

    Returns:
        pd.DataFrame: 카카오맵 정보가 추가된 데이터프레임.
    """
    if not isinstance(input_df, pd.DataFrame) or input_df.empty:
        print("입력 데이터가 비어있거나 DataFrame 형식이 아니므로 카카오 크롤링을 건너뜁니다.")
        return input_df

    print(f"카카오맵 크롤링 시작... (대상: {len(input_df)}개, 스레드: {max_threads}개)")
    store_list = input_df.to_dict("records")
    results = []

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(crawl_one, store, i % max_threads, headless): store
            for i, store in enumerate(store_list)
        }

        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                original_store = futures[future]
                print(f"매장 '{original_store.get('name', 'N/A')}' 처리 중 스레드 오류: {e}", file=sys.stderr)
                results.append(original_store)
            
            # 터미널에 진행 상황 표시
            print(f"\r- 진행률: {i}/{len(store_list)} ({ (i / len(store_list)) * 100:.1f}%)", end="")

    print("\n카카오맵 크롤링이 완료되었습니다.")
    return pd.DataFrame(results)

