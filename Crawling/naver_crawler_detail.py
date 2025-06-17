# 셀레니움 및 드라이버 모듈
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service
# from Age_balance_store_based_crawling import extract_demographic_data
from difflib import SequenceMatcher
from rapidfuzz import fuzz


from datetime import datetime, date
from urllib.parse import urlparse, parse_qs
from shapely.geometry import Point
import pandas as pd
import os
import sys
import time
import re
import logging
import random
import subprocess
import json

# 각종 util 함수
from .utils.get_instagram_link import get_instagram_link
from .utils.is_within_date import is_within_one_month, is_within_two_weeks, parse_date, is_within_three_months
from .utils.convert_str_to_number import convert_str_to_number
from .utils.haversine import haversine
from .utils.logger_utils import get_thread_logger

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12.6; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36",
    ]


class StoreCrawler:
    # 크롤링되는 features 리스트
    columns = ['location','name','naver_id','origin_name','category', 'new_store', 'instagram_link', 'instagram_post', 'instagram_follower',
               'visitor_review_count', 'blog_review_count', 'review_category','theme_mood','theme_topic','theme_purpose', 'menu_list', 'distance_from_subway', 'distance_from_subway_origin', 'on_tv',
               'parking_available', 'seoul_michelin', 'age-2030', 'gender-balance', 'gender_male', 'gender_female' ,'running_well', 'address', 'phone',
               'gps_latitude', 'gps_longitude','naver_url']  # menu_list가 중간에 포함되도록 수정
    
    def __init__(
            self, 
            location: str, 
            name: str,         # 매장명
            output_base_dir: str = None,
            headless: bool = True,
            save_threshold: int = 0,  # 추가
            thread_id=None,
    ):
        self.origin_name = name
        self.location = location
        self.search_word = name
        self.save_threshold = save_threshold 
        self.headless = headless  
        self.thread_id = thread_id

    
        #logger 먼저 정의
        self.logger = logging.getLogger(f"StoreCrawler_{name}")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # 나머지 초기화
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_base_dir = output_base_dir if output_base_dir else os.path.join(current_dir, 'result')
        os.makedirs(self.output_base_dir, exist_ok=True)  #디렉토리 생성 보장
        self.data = pd.DataFrame(columns=StoreCrawler.columns)
        self.user_agent_index = random.randint(0, len(USER_AGENTS) - 1)
                
        # 드라이버 초기화
        self.driver = self.init_driver()

        # 드라이버가 있는 경우에만 wait 객체 세팅
        if self.driver is not None:
            self.wait_short = WebDriverWait(self.driver, 2)
            self.wait_medium = WebDriverWait(self.driver, 5)
            self.wait = WebDriverWait(self.driver, 10)
        else:
            self.wait_short = self.wait_medium = self.wait = None

        # 네이버지도에서는 Iframe 태그를 통해서 매장 정보를 제공
        self.search_iframe = "searchIframe"
        self.entry_iframe = "entryIframe"
        self.empty_searchIframe = """//*[@id="_pcmap_list_scroll_container"]"""
        self.empty_entryIframe = """//*[@id="app-root"]"""
        self.empty_root = """//*[@id="root"]"""
        self.init_dictionary()

    def clear_store_dict(self):
        if not hasattr(self, 'store_dict') or not isinstance(self.store_dict, dict):
            self.logger.warning("store_dict가 존재하지 않거나 딕셔너리가 아님. 새로 초기화함.")
            self.init_dictionary()
            return
        for key in self.store_dict:
            self.store_dict[key] = None

    def init_driver(self):
        ua = USER_AGENTS[self.user_agent_index % len(USER_AGENTS)]
        self.logger.info(f"[UA] 현재 user-agent: {ua}")
        try:
            self.logger.info("FireFox Driver Options 설정 중...")
            options = FirefoxOptions()
            
            if self.headless:
                options.add_argument("--headless")
                self.logger.info("헤드리스 모드로 실행")
            else:
                self.logger.info("브라우저 창 표시 모드로 실행")
            
            options.add_argument("lang=ko_KR")
            # /dev/shm 파티션 사용 비활성화, Docker 같은 컨테이너 환경 에서 메모리 이슈 해결을 위함
            options.add_argument('--disable-dev-shm-usage')
            # GPU 하드웨어 가속 비활성화, 헤드리스 모드에서 성능 향상을 위함
            options.add_argument('--disable-gpu')
            # 브라우저 캐시 비활성화로 최신 데이터 로드
            options.set_preference("network.http.use-cache", False)
            # 크롤링 봇 탐지 회피를 위한 user agent 설정
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0')
            options.add_argument('--disable-blink-features=AutomationControlled')
            # 기타 옵션 설정
            options.add_argument("--window-size=1920,1080")  # 창 크기 고정
            # 파이어폭스 로깅 설정, 파이어폭스에는 직접 로그 경로를 설정하는 방법이 다를 수 있음
            options.log.level = "trace"
            options.set_preference("dom.security.https_first", False)
            options.set_preference("privacy.file_unique_origin", False)
            options.set_preference("network.cookie.cookieBehavior", 0)  # 모든 쿠키 허용

            self.logger.info("FireFox Driver 초기화 중...")
            # 도커 환경에서 파이어폭스의 드라이버인 geckodriver가 설치되는 절대 경로
            # geckodriver_path = '/opt/homebrew/bin/geckodriver'
            # service = Service(executable_path=geckodriver_path)
            service = Service() # geckodriver_path를 지정하지 않으면 PATH에서 자동으로 찾음
            driver = webdriver.Firefox(options=options, service=service)

            self.logger.info(f"네이버지도로 이동 중: {"https://map.naver.com/"}")
            driver.get('https://map.naver.com/')  # 수정: CSV에서 받은 naver_map_url로 이동
            driver.execute_script("window.open('');")  # 인스타그램 검색을 위해 여분의 탭을 열어둠
            driver.switch_to.window(driver.window_handles[0])  # 첫 번째 탭으로 전환
            return driver
        except Exception as e:
            self.logger.error("❌ WebDriver 초기화 실패", exc_info=True)
            return None  # ❗ 실패 시 반드시 None 반환

    def search_keyword(self):  #어떤 Frame으로 넘어가야 하는지 확인하기
        self.logger.info(f"{self.search_word} 검색어 입력 중...")
        self.move_to_default_content()
        time.sleep(1)
        try:
            # 🔸 검색창 입력
            search_box = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".input_search")))
            self.driver.execute_script("arguments[0].value = '';", search_box)
            time.sleep(0.5)
            search_box.send_keys(self.search_word)
            time.sleep(1)
            search_box.send_keys(Keys.RETURN)
            time.sleep(4.5)  # 검색 결과 로딩 대기

            self.logger.info("iframe 리스트에서 현재 상태 판단 시작")

            # 🔸 현재 페이지의 iframe들 확인
            iframe_elements = self.driver.find_elements(By.TAG_NAME, "iframe")
            iframe_ids = [iframe.get_attribute("id") for iframe in iframe_elements]

            if "entryIframe" in iframe_ids:
                self.logger.info("entryIframe 감지됨 (단일 매장)")
                return "entry"

            elif "searchIframe" in iframe_ids:
                self.logger.info("searchIframe 감지됨 (여러 후보 or 없음)")
                self.driver.switch_to.frame("searchIframe")
                time.sleep(1)

                # 🔸 "조건에 맞는 업체가 없습니다" 텍스트 확인 (FYvSc)
                #  => 검색할 때 SearchIframe, EntryIframe에 어떠한 관련 매장 정보가 뜨지 않고 조건에 맞는 업체가 없으라고 나오는 문구
                try:
                    no_result_elem = self.driver.find_element(By.CLASS_NAME, "FYvSc")
                    if "조건에 맞는 업체가 없습니다" in no_result_elem.text.strip():
                        self.logger.warning("❌ 조건에 맞는 업체 없음")
                        return "none"
                except Exception:
                    pass  # 없는 경우는 무시하고 후보 매장 있는 것으로 판단

                return "search"

            else:
                self.logger.warning("⚠️ entryIframe, searchIframe 모두 감지되지 않음")
                return "unknown"

        except Exception as e:
            self.logger.warning(f"❌ 검색어 입력 중 오류 발생: {e}")
            return "error"
        
    # Iframe 내부에 있을 때, 가장 상위의 frame으로 이동
    def move_to_default_content(self):
        self.driver.switch_to.default_content()
        # 이동 후 해당 frame의 빈 element를 클릭하여, 현재 frame이 제대로 이동했음을 확인
        self.wait.until(EC.presence_of_element_located(
            (By.XPATH, self.empty_root)))


    # 한 매장에 대한 크롤링을 마치고, 그 다음 매장을 크롤링 하기 위해 실행
    def init_dictionary(self):
        self.store_dict = {
            "origin_name": self.origin_name, 
            "location": self.location, 
            "naver_id": None,
            "name": None,
            "category": None,
            "new_store": False,
            "instagram_link": None,
            "instagram_post": None,
            "instagram_follower": None,
            "visitor_review_count": None,
            "blog_review_count": None,
            "review_category" : None,
            "theme_mood": None,          
            "theme_topic": None,         
            "theme_purpose": None,       
            "distance_from_subway": None,
            "distance_from_subway_origin":None, # 실제 어디 정거장에서 가져오는 건지 확인하기 위해서 
            "on_tv": None,
            "parking_available": False,
            "seoul_michelin": None,
            "age-2030": None,
            "gender-balance": None,
            "gender_male" : None,
            "gender_female" : None,
            "running_well": None,
            "address": None,
            "phone": None,
            "gps_latitude": None,  
            "gps_longitude": None,
            "Major category" : None,
            "naver_url": None,
            "menu_list": [],  
        }


    # "새로오픈" 태그를 가진 매장만을 겨냥한 크롤링 진행 시, 본격적인 매장 크롤링 직전에 실행
    def click_new_option(self):
        time.sleep(1)
        self.logger.info("새로오픈 태그 클릭")
        self.move_to_search_iframe()
        # "더보기" 버튼 클릭
        more_xpath = """//a[span[contains(text(),'더보기')]]"""
        more_button = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, more_xpath)))
        self.driver.execute_script("arguments[0].click()", more_button)
        # "새로오픈" 버튼 클릭
        new_xpath = """//a[contains(text(),'새로오픈')]"""
        new_button = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, new_xpath)))
        self.driver.execute_script("arguments[0].click()", new_button)

    def move_to_entry_iframe(self):
        """entryIframe으로 명시적으로 전환하는 함수"""
        self.move_to_default_content()
        iframe_element = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.ID, self.entry_iframe))
        )
        self.driver.switch_to.frame(iframe_element)


    def get_into_store(self) -> bool:
        """매장 진입 및 정보 수집"""
        try:
            self.init_dictionary()
            result = self.search_keyword()  
            if result == "none":
                self.logger.warning(f"❌ 검색 결과 없음: {self.search_word}")
                return False
            elif result == "error":
                self.logger.warning(f"❌ 검색 오류: {self.search_word}")
                return False
            elif result == "entry":
                return self.handle_entry_frame()
            elif result == "search":
                return self.handle_candidate_list_address_based()
            else:
                self.logger.warning(f"❌ 알 수 없는 프레임 상태: {result}")
                return False
        except Exception as e:
            self.logger.error(f"❌ 매장 진입 중 오류 발생: {e}")
            return False


    def handle_entry_frame(self) -> bool:
        try:
            self.init_dictionary()
            # 기본 값만 추가
            self.store_dict.setdefault("location", self.location)
            self.store_dict.setdefault("name", self.search_word)

            iframe_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "entryIframe"))
            )
            time.sleep(2)
            self.store_dict["naver_url"] = iframe_element.get_attribute("src")
            
            try:
                naver_id_str = urlparse(self.store_dict["naver_url"]).path.split('/')[2]
                self.store_dict["naver_id"] = int(naver_id_str)
            except (IndexError, ValueError):
                self.store_dict["naver_id"] = None
                self.logger.warning("❌ naver_id 파싱 실패 또는 숫자 변환 실패")

            # entryIframe으로 전환
            self.driver.switch_to.frame(iframe_element)
            time.sleep(2)

            # Apollo JSON에서 좌표 추출
            try:
                WebDriverWait(self.driver, 10).until(
                    lambda d: d.execute_script("return typeof window.__APOLLO_STATE__ !== 'undefined';")
                )
                
                apollo_json = self.driver.execute_script("return JSON.stringify(window.__APOLLO_STATE__);")
                if apollo_json:
                    apollo_data = json.loads(apollo_json)
                    place_detail = next((v for k, v in apollo_data.items() if k.startswith("PlaceDetailBase") and "coordinate" in v), None)
                    
                    if place_detail and "coordinate" in place_detail:
                        coordinate = place_detail["coordinate"]
                        x = float(coordinate["x"])
                        y = float(coordinate["y"])
                        self.store_dict["gps_latitude"] = y
                        self.store_dict["gps_longitude"] = x
                        self.logger.info(f"좌표 추출 완료 - 위도: {y}, 경도: {x}")
                    else:
                        self.logger.warning("❌ Apollo JSON에 좌표 데이터 없음")
                else:
                    self.logger.warning("❌ Apollo JSON 비어있음")
            except Exception as e:
                self.logger.warning(f"❌ Apollo JSON 파싱 실패: {e}")

            # 매장명 추출 및 검증
            try:
                name_xpath = """//*[@id="_title"]/div/span[1]"""
                title_elem = self.wait_medium.until(
                    EC.presence_of_element_located((By.XPATH, name_xpath))
                )
                actual_name = title_elem.text.strip()
                self.store_dict["name"] = actual_name
            except Exception as e:
                self.store_dict["name"] = self.origin_name
                self.logger.warning(f"❗ 매장명 추출 실패: {e}")

            # 매장명 불일치 시 실패 처리
            if self.origin_name not in self.store_dict["name"] and self.store_dict["name"] not in self.origin_name:
                self.logger.warning(f"❌ 매장명 불일치: {self.store_dict['name']} != {self.origin_name}")
                self.driver.switch_to.default_content()
                return False
            else:
                self.logger.info(f"최종 후보 매장 선택 완료: {self.store_dict['name']}")

            return True
        except Exception as e:
            self.logger.warning(f"❌ entryIframe 진입 실패: {e}")
            return False

    def handle_candidate_list_address_based(self) -> bool:

        try:
            def enter_search_frame():
                self.move_to_default_content()
                self.driver.switch_to.frame("searchIframe")
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(1.5)

            enter_search_frame()

            candidate_xpath = "//*[@id='_pcmap_list_scroll_container']/ul/li"
            page = 1
            best_score, best_page, best_idx, best_name, best_addr = -1, None, None, None, None

            while True:
                self.logger.info(f"[Page {page}] 후보 평가 시작")

                # — (1) 고정된 ID로 컨테이너 찾기
                try:
                    list_container = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.ID, "_pcmap_list_scroll_container"))
                    )
                except TimeoutException:
                    self.logger.warning("❌ 리스트 컨테이너 로딩 실패, 마지막 요소 스크롤로 대체")
                    list_container = None

                # — (2) 컨테이너 스크롤 안정화
                prev_height = -1
                if list_container:
                    while True:
                        time.sleep(2.5)
                        curr_height = self.driver.execute_script(
                            "return arguments[0].scrollHeight;", list_container
                        )
                        if curr_height == prev_height:
                            break
                        prev_height = curr_height
                        self.driver.execute_script(
                            "arguments[0].scrollTop = arguments[0].scrollHeight;", list_container
                        )
                else:
                    # fallback: 마지막 후보 요소로 스크롤
                    candidates = self.driver.find_elements(By.XPATH, candidate_xpath)
                    if candidates:
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView(true);", candidates[-1]
                        )
                        time.sleep(0.5)

                # — (3) 후보 로드 대기 & 수집
                WebDriverWait(self.driver, 15).until(
                    lambda d: len(d.find_elements(By.XPATH, candidate_xpath)) > 0
                )
                candidates = self.driver.find_elements(By.XPATH, candidate_xpath)
                total = len(candidates)
                self.logger.info(f"  페이지 {page}: 총 {total}개 후보")
                if total == 0:
                    self.logger.warning("후보가 하나도 없음")
                    return False

                # — (4) 이 페이지 최고 후보 계산
                page_best = (-1, None, None, None)
                for idx, el in enumerate(candidates):
                    try:
                        name_txt = el.find_element(By.CSS_SELECTOR, "div.ouxiq span.YwYLL").text.strip()
                        addr_txt = el.find_element(By.CSS_SELECTOR, "div.ouxiq span.Pb4bU").text.strip()
                    except:
                        continue

                    sim = SequenceMatcher(None, self.origin_name, name_txt).ratio()
                    tokens = self.location.split()
                    token_match = sum(1 for t in tokens if t in addr_txt)
                    loc_sim = token_match / len(tokens) if tokens else 0
                    score = sim * 0.8 + loc_sim * 0.2

                    if score > page_best[0]:
                        page_best = (score, idx, name_txt, addr_txt)

                # — (5) 글로벌 최고 갱신
                if page_best[0] > best_score:
                    best_score, best_page, best_idx, best_name, best_addr = (
                        page_best[0], page, page_best[1], page_best[2], page_best[3]
                    )
                    self.logger.info(
                        f"  ▶ [글로벌 갱신] 페이지{page} 최고: "
                        f"{best_name}/{best_addr} (점수={best_score:.2f}, 순서={best_idx}/{total-1})"
                    )

                # — (6) 다음 페이지로 이동 (mBN2s 클래스 활용)
                next_page = page + 1
                try:
                    link = self.driver.find_element(
                        By.XPATH,
                        f"//a[contains(@class,'mBN2s') and normalize-space(text())='{next_page}']"
                    )
                    prev_first = candidates[0]
                    link.click()
                    WebDriverWait(self.driver, 10).until(EC.staleness_of(prev_first))
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                            f"//a[contains(@class,'mBN2s qxokY') and normalize-space(text())='{next_page}']")
                        )
                    )
                    page += 1
                    enter_search_frame()
                    continue
                except:
                    break

            # ─ 2) 모든 페이지 끝난 뒤 최종 후보
            if best_score < 0:
                self.logger.warning("조건에 맞는 후보가 전혀 없음")
                return False
            self.logger.info(
                f"최종 선택: {best_name}/{best_addr} "
                f"(점수={best_score:.2f}), 페이지={best_page}, 인덱스={best_idx}"
            )

            # ─ 3) 저장된 페이지로 복귀
            enter_search_frame()
            try:
                link = self.driver.find_element(
                    By.XPATH,
                    f"//a[contains(@class,'mBN2s') and normalize-space(text())='{best_page}']"
                )
                prev_first = self.driver.find_elements(By.XPATH, candidate_xpath)[0]
                link.click()
                WebDriverWait(self.driver, 10).until(EC.staleness_of(prev_first))
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH,
                        f"//a[contains(@class,'mBN2s qxokY') and normalize-space(text())='{best_page}']")
                    )
                )
                enter_search_frame()
            except Exception as e:
                self.logger.warning(f"❌ 페이지 {best_page} 복귀 실패: {e}")

            # ─ 4) 복귀 후 스크롤 안정화 (ID 방식)
            prev_count = -1
            list_container = self.driver.find_element(By.ID, "_pcmap_list_scroll_container")
            while True:
                time.sleep(1)
                curr = self.driver.execute_script(
                    "return arguments[0].scrollHeight;", list_container
                )
                if curr == prev_count:
                    break
                prev_count = curr
                self.driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight;", list_container
                )

            candidates = self.driver.find_elements(By.XPATH, candidate_xpath)

            # ─ 5) 인덱스 대신 이름·주소 매칭 클릭
            target_el = None
            # 우선 인덱스 매칭
            if 0 <= best_idx < len(candidates):
                el = candidates[best_idx]
                nm = el.find_element(By.CSS_SELECTOR, "div.ouxiq span.YwYLL").text.strip()
                ad = el.find_element(By.CSS_SELECTOR, "div.ouxiq span.Pb4bU").text.strip()
                if nm == best_name and ad == best_addr:
                    target_el = el
            # 없으면 전체 검색
            if not target_el:
                for el in candidates:
                    nm = el.find_element(By.CSS_SELECTOR, "div.ouxiq span.YwYLL").text.strip()
                    ad = el.find_element(By.CSS_SELECTOR, "div.ouxiq span.Pb4bU").text.strip()
                    if nm == best_name and ad == best_addr:
                        target_el = el
                        break

            if not target_el:
                self.logger.warning("최종 후보 재매칭 실패")
                return False

            click_el = target_el.find_element(By.CSS_SELECTOR, "div.ouxiq span.YwYLL")
            self.driver.execute_script("arguments[0].scrollIntoView(true);", click_el)
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.ouxiq span.YwYLL"))
            )
            self.driver.execute_script("arguments[0].click();", click_el)
            time.sleep(2)

            # ─ 6) 기존 로직 유지: entryIframe 진입 및 좌표/ID 추출
            try:
                self.move_to_default_content()
                iframe_el = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.ID, "entryIframe"))
                )
                iframe_src = iframe_el.get_dom_attribute("src")
                self.store_dict["naver_url"] = iframe_src

                match = re.search(r'/place/(\d+)', iframe_src)
                self.store_dict["naver_id"] = int(match.group(1)) if match else None

                self.driver.switch_to.frame(iframe_el)
            except TimeoutException:
                self.logger.warning("❌ entryIframe 로딩 실패 (Timeout)")
                return False
            except Exception as e:
                self.logger.warning(f"❌ entryIframe 전환 실패: {e}")
                self.store_dict["naver_url"] = None
                self.store_dict["naver_id"] = None
                return False

            WebDriverWait(self.driver, 15).until(
                lambda d: d.execute_script("return !!window.__APOLLO_STATE__;")
            )
            ap = self.driver.execute_script("return JSON.stringify(window.__APOLLO_STATE__);")
            if ap:
                data = json.loads(ap)
                detail = next(
                    (v for k, v in data.items()
                    if k.startswith("PlaceDetailBase") and "coordinate" in v),
                    None
                )
                if detail:
                    y, x = detail["coordinate"]["y"], detail["coordinate"]["x"]
                    self.store_dict["gps_latitude"] = float(y)
                    self.store_dict["gps_longitude"] = float(x)
                    self.logger.info(f"좌표: {y}, {x}")
                else:
                    self.logger.warning("Apollo에 좌표 없음")
            else:
                self.logger.warning("Apollo JSON이 비어있음")

            return True

        except Exception as e:
            self.logger.warning(f"❌ 후보 판단 중 오류: {e}")
            return False

    # 탭 이동 후 직접 time sleep을 사용해 대시한다
    def move_to_tab(self, tab_name):
        tab_xpath = f"""//a[@role='tab' and .//span[text()='{tab_name}']]"""
        # tab_element = self.driver.find_element(By.XPATH, tab_xpath)
        tab_element = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, tab_xpath))
        )        
        self.driver.execute_script("arguments[0].click()", tab_element)
        time.sleep(2)
    
    # 한 매장에 대한 정보 얻는 과정
    def get_store_details(self):
        time.sleep(2)
        self.logger.info("매장 정보 크롤링 중...")

        # <GPS를 바탕으로 확인하는 과정>
        try:
            if self.store_dict["gps_longitude"] is None or self.store_dict["gps_latitude"] is None:
                raise ValueError("GPS 좌표가 None입니다.")
            store_point = Point(float(self.store_dict["gps_longitude"]), float(self.store_dict["gps_latitude"]))
        except Exception as e:
            self.logger.warning("❌ GPS 정보가 없어 경도,위도 확인 불가")
            self.logger.warning(f"Error: {e}")        
        

        # <매장 이름, 카테고리> 추출 및 저장
        try:
            store_name_xpath = """//*[@id="_title"]/div/span"""
            title_element = self.wait_medium.until(EC.presence_of_all_elements_located(
                (By.XPATH, store_name_xpath)))

            self.store_dict['name'] = title_element[0].text
            self.store_dict['category'] = title_element[1].text
            
            # <새로오픈> 여부 확인
            if len(title_element) > 2:
                third_span = title_element[2]
                self.logger.info(f"span[3] 텍스트: {third_span.text}, 클래스: {third_span.get_attribute('class')}")
                if third_span.text.strip() == "새로오픈" and "PI7f0" in third_span.get_attribute("class"):
                    self.store_dict['new_store'] = True
                    self.logger.info(f"새로오픈 매장 확인: {self.store_dict['name']}")
                else:
                    self.store_dict['new_store'] = False
            else:
                self.store_dict['new_store'] = False
        
        except TimeoutException as e:
            self.logger.warning("❌ 매장 이름, 카테고리, 새로오픈 여부 확인 중 TimeoutException 발생")
            self.logger.warning(e)
            return False
        except Exception as e:
            self.logger.warning("❌ 매장 이름, 카테고리, 새로오픈 여부 확인 중 에러 발생")
            self.logger.warning(e)
            return False


        # <인스타그램 계정 추출 및 저장> -> 여기서 확인 후에 추후에 자세한 정보 탭에서 팔로워 수, 포스트 수 확인
        try:
            elem = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'instagram.com')]"))
            )
            instagram_url = elem.get_attribute('href')
            result = get_instagram_link(instagram_url)

            # 인스타그램 계정 url이 올바르지 않은 경우
            if result == None:
                self.store_dict['instagram_link'] = None
                self.store_dict['instagram_post'] = None
                self.store_dict['instagram_follower'] = None
            # 올바른 경우
            elif result != None:
                self.store_dict['instagram_link'] = result
            else:
                self.store_dict['instagram_link'] = None
                self.store_dict['instagram_post'] = None
                self.store_dict['instagram_follower'] = None
    
        # 매장이 네이버지도에 인스타그램 계정을 등록해두지 않은 경우
        except (NoSuchElementException, TimeoutException) as e:
            self.store_dict['instagram_link'] = None
            self.store_dict['instagram_post'] = None
            self.store_dict['instagram_follower'] = None
        except Exception as e:
            self.logger.warning("❌ 인스타그램 크롤링 실패")
            self.logger.warning(e)
            self.store_dict['instagram_link'] = None
            self.store_dict['instagram_post'] = None
            self.store_dict['instagram_follower'] = None
   

        # <주소 저장>
        try: 
            address_xpath = "//strong[contains(.,'주소')]/following-sibling::div/a/span"
            address_elem = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, address_xpath))
            )
            # 요소가 headless 모드에서도 화면에 보이도록 스크롤 이동
            self.driver.execute_script("arguments[0].scrollIntoView(true);", address_elem)
            time.sleep(1)  # 추가 대기 (동적 렌더링 보완)

            # 요소의 텍스트를 추출 (headless 모드에서 .text가 비어있을 수 있으므로 JS로도 추출)
            address_text = address_elem.text.strip()
            if not address_text:
                address_text = self.driver.execute_script("return arguments[0].textContent;", address_elem).strip()
            if address_text:
                self.store_dict["address"] = address_text
            else:
                self.store_dict["address"] = None

            # 💡 여기서 location 자동 보정
            if not self.location or self.location.strip() == "":
                gu_match = re.search(r"(서울특별시\s)?([가-힣]+구)", address_text)
                self.location = gu_match.group(2) if gu_match else "unknown"


        except NoSuchElementException as e:
            self.store_dict["address"] = None
        except Exception as e:
            self.logger.warning("❌ 도로명 주소 크롤링 실패")
            self.logger.warning(e)
            self.store_dict["address"] = None


        # <매장 전화번호>
        try:
            phone_xpath = "//strong[contains(.,'전화번호')]/following-sibling::div/span"
            phone_elem = self.driver.find_element(By.XPATH, phone_xpath)
            phone_text = phone_elem.text
            if phone_text != "":
                self.store_dict["phone"] = phone_text
        except NoSuchElementException:
            self.store_dict["phone"] = None
        except Exception as e:
            self.logger.warning("❌ 매장 전화번호 크롤링 실패")
            self.logger.warning(e)
            self.store_dict["phone"] = None


        # <서울 미쉐린 가이드 등재 여부> 확인 및 저장
        try:
            self.move_to_tab("홈")
            time.sleep(2)
            # "미쉐린 가이드 서울" 텍스트를 포함하는지 여부로 확인
            michelin_xpath = """//div[a[contains(text(), '미쉐린 가이드 서울')]]"""
            self.driver.find_element(By.XPATH, michelin_xpath)
            self.store_dict['seoul_michelin'] = True
        except NoSuchElementException:
            self.store_dict['seoul_michelin'] = False
        except Exception as e:
            # self.logger.warning("서울 미쉐린 가이드 크롤링 실패")
            # self.logger.warning(e)
            self.store_dict['seoul_michelin'] = False


        # <지하철역 출구로부터 거리 추출 및 저장>
        try:
            subway_xpath = "/html/body/div[3]/div/div/div/div[5]/div/div[2]/div[1]/div/div[1]/div/div"
            elem = self.driver.find_element(By.XPATH, subway_xpath)
            text = elem.text.replace('\n', ' ').replace('\r', ' ')
            self.store_dict["distance_from_subway_origin"] = text

            numbers = re.findall(r'\d+', text)
            if numbers:
                self.store_dict["distance_from_subway"] = convert_str_to_number(
                    numbers[-1])
            else:
                self.store_dict["distance_from_subway"] = None

        except NoSuchElementException:
            self.store_dict["distance_from_subway"] = None
            self.store_dict["distance_from_subway_origin"] = None

        except Exception as e:
            self.logger.warning("❌ 지하철역으로부터 매장까지 거리 크롤링 실패")
            self.logger.warning(e)
            self.store_dict["distance_from_subway"] = None
            self.store_dict["distance_from_subway_origin"] = None

        # <방송 출연 여부> 확인 및 저장
        try:
            tv_xpath = """//strong[descendant::span[text()='TV방송정보']]"""
            self.driver.find_element(By.XPATH, tv_xpath)
            self.store_dict['on_tv'] = True
        except NoSuchElementException:
            self.store_dict['on_tv'] = False
        except Exception as e:
            self.logger.warning("❌ 방송 출연 여부 크롤링 실패")
            self.logger.warning(e)
            self.store_dict['on_tv'] = False

        # <주차 가능> 확인 및 저장
        try:
            convenient_xpath = "//strong[descendant::span[text()='편의']]/ancestor::div[1]/div/div"
            elem = self.driver.find_element(By.XPATH, convenient_xpath)
            convenients = elem.text

            for parking in ["주차", "발렛파킹"]:
                if parking in convenients:
                    self.store_dict["parking_available"] = True
                    break

        except NoSuchElementException:
            self.store_dict["parking_available"] = False
        except Exception as e:
            self.logger.warning("❌ 주차, 여부 크롤링 실패")
            self.logger.warning(e)
            self.logger.info(f"주차, 반려동물, 에러: {e}")
            self.store_dict["parking_available"] = False


        # 홈 탭에 있는 영역 크롤랑 # 
        try:
            # '홈' 탭으로 이동 후 스크롤로 모든 콘텐츠 로드
            self.move_to_tab('홈') 
            # 마지막 스크롤을 한 이후에 
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.5)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # 1. "데이터랩" 섹션까지 스크롤
            datalab_xpath = "//span[@class='place_blind' and contains(text(), '데이터랩')]"
            datalab_elem = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, datalab_xpath))
            )
            self.driver.execute_script("arguments[0].scrollIntoView(true);", datalab_elem)


            # 2. 데이터랩 전체 영역 div(place_section I_y6k) 찾기
            datalab_section = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.place_section.I_y6k"))
            )

            # 키워드 요소 찾기
            theme_keyword_xpath = "//h3[contains(text(), '테마키워드')]"
            theme_keyword_elem = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, theme_keyword_xpath))
            )
            self.logger.info(f"테마키워드 HTML: {theme_keyword_elem.get_attribute('outerHTML')}")

            # 테마키워드 수집
            try:
                theme_container = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, ".//div[@class='WXrhH']"))
                )
                li_elements = theme_container.find_elements(By.XPATH, ".//ul[@class='v4tIa']/li")

                theme_by_category = {
                    "분위기": [],
                    "인기토픽": [],
                    "찾는목적": []
                }
                for li in li_elements:
                    # 메인 카테고리
                    main_category = li.find_element(By.CLASS_NAME, "pNnVF").text.strip()
                    # 세부 키워드 추출
                    if main_category in theme_by_category:  # 조건 추가
                        sub_items = li.find_elements(By.XPATH, ".//span[@class='sJgQj']/span")
                        extracted = [s.text.replace(",", "").strip() for s in sub_items if s.text.strip()]
                        theme_by_category[main_category].extend(extracted)

                # 결과 저장
                # 개별 항목별로 store_dict에 저장
                self.store_dict["theme_mood"] = theme_by_category["분위기"]
                self.store_dict["theme_topic"] = theme_by_category["인기토픽"]
                self.store_dict["theme_purpose"] = theme_by_category["찾는목적"]

            except Exception as e:
                self.logger.warning("❌ 테마키워드 수집 실패")
                self.store_dict["theme_mood"] = []
                self.store_dict["theme_topic"] = []
                self.store_dict["theme_purpose"] = []
                self.logger.info(f"테마키워드 수집 실패: {e}")
                
            # '더보기' 버튼 클릭
            for attempt in range(5):  # 최대 2회 시도
                try:
                    self.logger.info(f"더보기 버튼 클릭 시도 {attempt + 1}회차")
                    button_elem = WebDriverWait(datalab_section, 10).until(
                        EC.element_to_be_clickable((By.XPATH, ".//div[contains(@class, 'NSTUp')]//span[contains(text(), '더보기')]"))
                    )
                    # 🔹 스크롤 내리고 클릭 재시도
                    for _ in range(3):
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", button_elem)
                        time.sleep(0.5)
                        self.driver.execute_script("arguments[0].click();", button_elem)
                        # 클릭 성공했으면 루프 탈출
                        if "expanded" in button_elem.get_attribute("class"):
                            break

                    # 🔹 클릭 후 확장된 UI 요소가 등장할 때까지 대기
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@class='WXrhH']"))
                    )
                    self.logger.info("테마 키워드 확장 완료!")

                    break  #성공했으니 재시도 루프 탈출!

                except (TimeoutException, NoSuchElementException) as e:
                    self.logger.warning(f"❌ 더보기 버튼 클릭 실패 (시도 {attempt + 1})")
                    self.logger.warning(e)
                    time.sleep(2)  # 실패 시 약간 대기 후 재시도
                except Exception as e:
                    self.logger.warning("❌ Datalab 더보기 버튼 클릭 중 예기치 못한 오류")
                    self.logger.warning(e)
                    break  # 예상 못한 에러면 반복 안 하고 탈출

            try:
                # 🔹 **연령별 데이터를 포함하는 div.gZ4G4 요소 찾기**
                gender_age_container_xpath = "//div[contains(@class, 'gZ4G4')]"
                if len(self.driver.find_elements(By.XPATH, gender_age_container_xpath)) == 0:
                    self.logger.warning("연령별 비율 정보가 없음, 기본값 None 설정")
                    self.store_dict["age-2030"] = None

                else:
                    gender_age_container = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, gender_age_container_xpath))
                )
                self.driver.execute_script("arguments[0].scrollIntoView(true);", gender_age_container)
                time.sleep(2)

                # 연령별 데이터 추출: bar_chart_container의 ul.Pu5eW가 보일 때까지 대기
                ul_locator = (By.CSS_SELECTOR, "#bar_chart_container > ul.Pu5eW")
                WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_element_located(ul_locator)
                )

                # 여러 번 스크롤해서 모든 li 요소가 로드되도록 시도 (최대 3회 반복)
                for _ in range(2):
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                li_elements = self.driver.find_elements(
                    By.CSS_SELECTOR, "#bar_chart_container > ul.Pu5eW > li.JkrLe"
                )

                if len(li_elements) < 6:
                    self.logger.warning("연령별 li 요소가 6개 미만, 데이터 부족으로 간주.")
                    self.store_dict["age-2030"] = 0
                else:
                    scores = [0] * 6  # 10대,20대,30대,40대,50대,60대
                    for idx, li in enumerate(li_elements):
                        # 해당 li를 스크롤해서 보이게 하고 추가 대기
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", li)
                        time.sleep(0.7)
                        try:
                            # span.NwNob에서 텍스트 추출 (text 없으면 textContent 재시도)
                            value_span = li.find_element(By.CSS_SELECTOR, "span.NwNob")
                            text = value_span.text.strip()
                            if not text:
                                text = self.driver.execute_script("return arguments[0].textContent;", value_span).strip()
                            scores[idx] = int(text) if text.isdigit() else 0
                        except Exception as err:
                            # self.logger.warning(f"[{idx}]번째 li에서 점수 추출 실패: {err}")
                            scores[idx] = 0

                    score_20 = scores[1]
                    score_30 = scores[2]
                    # 최종 점수 계산: 둘 다 1 또는 2면 2, 둘 중 하나만 1 또는 2면 1, 아니면 0
                    if (score_20 in [1, 2]) and (score_30 in [1, 2]):
                        final_score = 2
                    elif (score_20 in [1, 2]) or (score_30 in [1, 2]):
                        final_score = 1
                    else:
                        final_score = 0

                    self.store_dict["age-2030"] = final_score
                    self.logger.info(f"연령별 점수 계산 완료: 20대={score_20}, 30대={score_30}, 최종={final_score}")

            except Exception as e:
                self.logger.warning("연령별 비율 계산 실패")
                self.logger.warning(e)
                self.store_dict["age-2030"] = None


            # 성별 데이터 추출 수정 후
            try:
             
                self.driver.execute_script("return document.readyState") == "complete"

                # 직접 JS로 접근
                male_text = self.driver.execute_script("""
                    const el = document.querySelector('#_datalab_chart_donut1_0')
                                    ?.querySelector('g.c3-target-male text');
                    if (el) {
                        el.style.display = 'block';
                        return el.textContent.trim();
                    }
                    return null;
                """)
                
                female_text = self.driver.execute_script("""
                    const el = document.querySelector('#_datalab_chart_donut1_0')
                                    ?.querySelector('g.c3-target-female text');
                    if (el) {
                        el.style.display = 'block';
                        return el.textContent.trim();
                    }
                    return null;
                """)

                self.logger.info(f"male_text: {male_text}, female_text: {female_text}")


                # 🔹 텍스트 처리 및 결과 저장 (기존 코드 유지)
                try:
                    male = round(float(male_text), 0) if male_text.replace(".", "").isdigit() else 0
                    female = round(float(female_text), 0) if female_text.replace(".", "").isdigit() else 0
                except ValueError:
                    self.logger.warning(f"⚠️ 숫자 변환 실패 - 남성: {male_text}, 여성: {female_text}")
                    male, female = 0, 0

                self.store_dict["gender_male"] = male
                self.store_dict["gender_female"] = female
                self.store_dict["gender-balance"] = (male < 55)
                self.logger.info(f"최종 성별 비율 - 남성: {male}%, 여성: {female}%")
                self.logger.info(f"gender-balance: {'균형 잡힘 (True)' if male < 55 else '균형 안 잡힘 (False)'}")

              
            except TimeoutException:
                self.logger.warning("⛔ 성별 데이터 찾기 실패. 기본값 설정")
                self.store_dict["gender-balance"] = None
                self.store_dict["gender_male"] = None
                self.store_dict["gender_female"] = None
            except Exception as e:
                self.logger.warning("성별 데이터 처리 중 오류 발생")
                self.logger.warning(e)
                self.store_dict["gender-balance"] = None
                self.store_dict["gender_male"] = None
                self.store_dict["gender_female"] = None  

        except NoSuchElementException as e:
            # 요소 탐색 실패 시 기본값 설정
            self.store_dict["age-2030"] = None
            self.store_dict["gender-balance"] = None
            self.store_dict["gender_male"] = None
            self.store_dict["gender_female"] = None
            self.logger.warning("요소 탐색 실패.")
        except Exception as e:
            # 예기치 않은 오류 처리
            self.store_dict["age-2030"] = None
            self.store_dict["gender-balance"] = None
            self.store_dict["gender_male"] = None
            self.store_dict["gender_female"] = None
            

## Data Lab 추출 완료 --------------------

        # 방문자 리뷰, 블로그 리뷰 개수 추출 및 저장
        try:
            # 방문자 리뷰
            elem_visitor = self.driver.find_element(
                By.XPATH, value="//a[contains(text(), '방문자 리뷰')]")
            visitor_review_count = int(re.findall(
                r'\d+', elem_visitor.text.replace(",", ""))[0])
            self.store_dict['visitor_review_count'] = visitor_review_count
        except NoSuchElementException:
            self.store_dict['visitor_review_count'] = 0
        except Exception as e:
            self.logger.warning("방문자 리뷰 크롤링 실패")
            self.logger.warning(e)

        try:
            # 블로그 리뷰
            elem_blog = self.driver.find_element(
                By.XPATH, value="//a[contains(text(), '블로그 리뷰')]")
            blog_review_count = int(re.findall(
                r'\d+', elem_blog.text.replace(",", ""))[0])
            time.sleep(random.uniform(0.5, 2.5))
            self.store_dict['blog_review_count'] = blog_review_count
        except NoSuchElementException:
            self.store_dict['blog_review_count'] = 0
        except Exception as e:
            self.logger.warning("❌ 블로그 리뷰 크롤링 실패")
            self.logger.warning(e)

        # 메뉴 탭으로 이동 및 메뉴 정보 크롤링
        try:
            self.move_to_tab("메뉴")
            time.sleep(2)

            # 신규 스마트주문 구조] 존재 여부 확인
            if self.driver.find_elements(By.CSS_SELECTOR, "div.order_list_wrap.order_list_category.store_delivery"):
                self.logger.info("📦 스마트주문 메뉴 구조 감지")
                detail_blocks = self.driver.find_elements(By.CSS_SELECTOR, "div.info_detail")
                menu_items = []
                for block in detail_blocks:
                    try:
                        is_representative = bool(block.find_elements(By.CSS_SELECTOR, "span.menu_tag.default"))

                        name_elem = block.find_element(By.CSS_SELECTOR, "div.tit")
                        menu_name = name_elem.text.strip()

                        intro_elem = block.find_element(By.CSS_SELECTOR, "span.detail_txt")
                        menu_intro = intro_elem.text.strip().replace("\\/", "/")

                        price_elem = block.find_element(By.CSS_SELECTOR, "div.price")
                        menu_price = price_elem.text.strip()

                        menu_items.append({
                            "name": menu_name,
                            "intro": menu_intro,
                            "price": menu_price,
                            "is_representative": is_representative
                        })
                    except Exception as e:
                        self.logger.warning(f"⚠️ 스마트주문 메뉴 블록 파싱 실패: {e}")
                        continue

            else:
                self.logger.info("기본 메뉴 구조 사용")
                menu_ul = self.driver.find_element(By.CSS_SELECTOR, "div.place_section_content > ul")
                li_elements = menu_ul.find_elements(By.CSS_SELECTOR, "li.E2jtL")
                self.logger.info(f"li_elements 개수: {len(li_elements)}")

                menu_items = []
                for li in li_elements:
                    try:
                        is_representative = False
                        try:
                            rep_elem = li.find_element(By.CSS_SELECTOR, "span.QM_zp > span.place_blind")
                            if rep_elem.text.strip() == "대표":
                                is_representative = True
                        except NoSuchElementException:
                            pass  # 대표 표시 없는 경우 무시

                        # 메뉴명
                        menu_name = li.find_element(By.CSS_SELECTOR, "span.lPzHi").text.strip()

                        # 메뉴 설명 (kPogF 없을 수 있음)
                        try:
                            menu_intro = li.find_element(By.CSS_SELECTOR, "div.kPogF").text.strip()
                        except NoSuchElementException:
                            menu_intro = None

                        # 가격
                        try:
                            menu_price = li.find_element(By.CSS_SELECTOR, "div.GXS1X").text.strip()
                        except NoSuchElementException:
                            menu_price = None

                        # 저장
                        menu_items.append({
                            "name": menu_name,
                            "intro": menu_intro,
                            "price": menu_price,
                            "is_representative": is_representative
                        })
                    except Exception as e:
                        self.logger.warning(f"⚠️ 일반 메뉴 항목 파싱 실패: {e}")
                        continue

            self.store_dict["menu_list"] = menu_items
            # self.logger.info(f"🍽️ 메뉴 정보: {menu_items}")

        except Exception as e:
            self.logger.warning("메뉴 탭 크롤링 실패")
            self.logger.warning(e)
            self.store_dict["menu_list"] = []

       
        # 리뷰 수집
        try:
            self.driver.switch_to.default_content()
            self.move_to_entry_iframe()

            tab_xpath = """//a[@role='tab']//span[text()='리뷰']"""
            self.wait_short.until(EC.presence_of_element_located((By.XPATH, tab_xpath)))
            self.logger.info("리뷰 탭 존재 확인됨")

            # 리뷰 탭 클릭
            self.move_to_tab('리뷰')

            # 리뷰 콘텐츠 렌더링 대기
            self.wait_medium.until(
                EC.presence_of_element_located((By.XPATH, '//li[contains(@class, "place_apply_pui")]'))
            )
            self.logger.info("리뷰 탭 콘텐츠 렌더링 완료됨")

            # 최신순 정렬 시도
            try:
                latest_sort_elem = self.wait_medium.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "div.mlywZ > span.v6aH1:nth-child(2) > a"))
                )
                self.driver.execute_script("arguments[0].click();", latest_sort_elem)
                self.logger.info("최신순 정렬 버튼 클릭 완료")
            except Exception as e:
                self.logger.warning(f"❌ 최신순 클릭 실패: {e}")

            # 리뷰 키워드 수집
            data = {}
            try:
                review_items = self.wait_medium.until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.MHaAm"))
                )
                for item in review_items:
                    try:
                        category = item.find_element(By.CSS_SELECTOR, "span.t3JSf").text.strip().replace('"', '')
                        score_text = item.find_element(By.CSS_SELECTOR, "span.CUoLy").text.strip()
                        score_text = score_text.replace("이 키워드를 선택한 인원", "").strip()
                        score = int(score_text) if score_text.isdigit() else 0
                        data[category] = score
                    except Exception as e:
                        self.logger.warning(f"❌ 키워드 파싱 실패: {e}")
                        continue
            except Exception as e:
                self.logger.warning("❌ 리뷰 키워드 수집 실패")
                self.logger.warning(e)
            self.store_dict['review_category'] = data
            self.logger.info(f"리뷰 키워드: {data}")

            # 방문자 리뷰 날짜/댓글 수집
            review_info = []
            try:
                review_elements = self.driver.find_elements(By.XPATH, '//li[contains(@class, "place_apply_pui")]')
                for li in review_elements:
                    try:
                        date_elem = li.find_element(By.XPATH, './/span[contains(text(), "방문일")]/following-sibling::span')
                        date_text = date_elem.text.strip()
                        parsed_date = parse_date(date_text)

                        comment_elems = li.find_elements(By.XPATH, './/div[contains(@class, "pui__vn15t2")]//a')
                        comment_texts = [a.text.strip() for a in comment_elems if a.text.strip()]
                        comment_text = " ".join(comment_texts).replace("\n", " ").replace("더보기", "").strip()

                        if parsed_date:
                            date_str = parsed_date.strftime("%Y-%m-%d") if isinstance(parsed_date, (datetime, date)) else str(parsed_date)
                            review_info.append({"date": date_str, "comment": comment_text})
                    except Exception:
                        continue
            except Exception as e:
                self.logger.warning("❌ 리뷰 댓글 수집 실패")
                self.logger.warning(e)

            review_info = sorted(review_info, key=lambda x: x["date"], reverse=True)[:5]
            self.store_dict["review_info"] = review_info
            self.store_dict["crawling_date"] = datetime.now().strftime("%Y-%m-%d")
            self.logger.info(f"수집된 리뷰(날짜+댓글): {review_info}")

            # 운영 상태 평가
            self.store_dict['running_well'] = 0
            visit_review_dates = []
            for item in review_info:
                try:
                    d = item["date"]
                    visit_review_dates.append(d if isinstance(d, date) else datetime.strptime(d, "%Y-%m-%d").date())
                except Exception:
                    continue

            if not visit_review_dates:
                self.logger.warning("리뷰 데이터 없음 → 운영 상태 평가: 0")
            elif not any(is_within_three_months(d) for d in visit_review_dates):
                self.logger.warning("최근 3개월 내 방문 없음 → 운영 상태 평가: 0")
            elif any(is_within_one_month(d) for d in visit_review_dates):
                self.store_dict["running_well"] = 3 if any(is_within_two_weeks(d) for d in visit_review_dates) else 2
            else:
                self.store_dict["running_well"] = 1

        except Exception as e:
            self.logger.warning("❌ 리뷰 탭 전체 수집 실패")
            self.logger.warning(e)
            self.store_dict["review_category"] = ''
            self.store_dict["review_info"] = []
            self.store_dict["running_well"] = 0

        # 인스타그램 게시글 수, 팔로워 수 추출 및 저장
        if self.store_dict['instagram_link'] != None:  # 인스타그램 계정이 있는 경우에만 실행
            try:
                instagram_embed_url = self.store_dict['instagram_link'] + "/embed"

                # 인스타그랩 탭으로 이동
                self.driver.switch_to.window(self.driver.window_handles[1])
                self.driver.get(instagram_embed_url)

                name_xpath = """/html/body/div/div/div/div/div/div/div/div/div[1]/div[2]/div[1]/a/div"""
                self.wait_medium.until(EC.presence_of_element_located(
                    (By.XPATH, name_xpath)))

                #  insta follower , post 위치 변경 반영
                follower_xpath = """/html/body/div/div/div/div/div/div/div/div/div[1]/div[2]/div[3]/span/div[1]/span/span"""
                post_xpath = """/html/body/div/div/div/div/div/div/div/div/div[1]/div[2]/div[3]/span/div[2]/span/span"""

                follower_elem = self.driver.find_element(
                    By.XPATH, follower_xpath)

                post_elem = self.driver.find_element(By.XPATH, post_xpath)

                follower = convert_str_to_number(follower_elem.text)
                post = convert_str_to_number(post_elem.text)
                self.logger.info(f'follower : {follower}, post : {post}')
                self.store_dict["instagram_follower"] = follower
                self.store_dict["instagram_post"] = post
            except (NoSuchElementException, TimeoutException, WebDriverException):
                self.store_dict['instagram_link'] = None
                self.store_dict["instagram_follower"] = None
                self.store_dict["instagram_post"] = None
            except Exception as e:
                self.logger.warning("❌ 인스타그램 크롤링 실패")
                self.logger.warning(e)
                self.store_dict['instagram_link'] = None
                self.store_dict["instagram_follower"] = None
                self.store_dict["instagram_post"] = None
        
        # 네이버지도 탭으로 복귀
        self.driver.switch_to.window(self.driver.window_handles[0])
        self.move_to_entry_iframe()

        # # 한 매장에 대한 크롤링 결과
        self.logger.info(f"""{self.location}, 매장 이름: {self.store_dict["name"]}, 매장 카테고리: {
            self.store_dict["category"]}""")
        self.insert_into_dataframe()
        return True



    def restart_driver_inline(self):
        """드라이버 재시작이 필요한 경우에만 호출"""
        try:
            if self.driver:
                self.driver.quit()
            self.logger.info("🚀 WebDriver 재초기화 중...")
            self.driver = self.init_driver()
            
            if self.driver is not None:
                self.wait_short = WebDriverWait(self.driver, 2)
                self.wait_medium = WebDriverWait(self.driver, 5)
                self.wait = WebDriverWait(self.driver, 10)
                self.logger.info("WebDriver 재초기화 완료")
            else:
                self.wait_short = self.wait_medium = self.wait = None
                self.logger.error("❌ WebDriver 재초기화 실패")
        except Exception as e:
            self.logger.error(f"❌ WebDriver 재초기화 중 오류 발생: {e}")
            self.driver = None
            self.wait_short = self.wait_medium = self.wait = None

    def insert_into_dataframe(self):
        """
        현재 store_dict의 데이터를 new_data로 변환하여 self.data에 추가한다.
        중간 저장 조건이 충족되면 저장도 수행한다.
        """

        if not self.store_dict:
            self.logger.warning("store_dict가 비어 있어 데이터프레임으로 변환하지 않음")
            return

        try:
            new_data = pd.DataFrame([self.store_dict])
        except Exception as e:
            self.logger.warning(f"❌ store_dict를 DataFrame으로 변환하는 데 실패함: {e}")
            return

        if new_data.empty or new_data.isna().all(axis=1).iloc[0]:
            self.logger.warning("new_data가 비어 있거나 모든 값이 NaN이므로 self.data에 추가하지 않음")
            return

        if not hasattr(self, "data") or self.data is None:
            self.logger.info("self.data가 존재하지 않아서 new_data로 초기화합니다.")
            self.data = new_data
        elif self.data.empty or self.data.isna().all().all():
            self.logger.info("self.data가 empty 또는 모두 NaN이므로 new_data로 초기화합니다.")
            self.data = new_data
        else:
            try:
                self.data = pd.concat([self.data, new_data], ignore_index=True)
                self.logger.info(f"self.data에 매장 정보를 추가함. 현재 행 개수: {len(self.data)}")
            except Exception as e:
                self.logger.warning(f"self.data에 new_data를 추가하는 데 실패함: {e}")
                return


    def crawling_single_page(self, page):
        self.move_to_search_iframe()
        store_count = self.scroll_to_end()  # 현재 페이지의 매장 수 확인

        self.logger.info(f"{page} 번째 페이지 크롤링 시작...")
        for i in range(1, store_count + 1):
            self.logger.info(f"{page} 페이지 {i} 번째 매장 크롤링 중...")

            try:
                # 매장 진입 성공한 경우에만 정보 크롤링
                success = self.get_into_store()
                time.sleep(0.5)
                if success:
                    self.get_store_details()
                else:
                    self.logger.info(f"{i} 번째 매장은 중복 혹은 진입 실패로 건너뜀")
                    continue

            except Exception as e:
                # 어떤 오류가 발생하더라도 로그를 남기고 다음 매장으로 이동
                self.logger.error(f"{i} 번째 매장 크롤링 중 오류 발생: {e}")
                continue  # 다음 매장으로 진행

        self.logger.info(f"{page} 페이지 크롤링 완료")


    def scroll_to_end(self):
        try:
            li_xpath = """//*[@id="_pcmap_list_scroll_container"]/ul/li"""
            store_elements = self.wait.until(
                EC.presence_of_all_elements_located((By.XPATH, li_xpath)))
            store_count = len(store_elements)
            self.driver.execute_script(
                "arguments[0].scrollIntoView(true);", store_elements[-1])

            while True:
                time.sleep(0.5)
                store_elements = self.wait.until(
                    EC.presence_of_all_elements_located((By.XPATH, li_xpath)))
                new_store_count = len(store_elements)

                if store_count == new_store_count:
                    break
                store_count = new_store_count
                self.driver.execute_script(
                    "arguments[0].scrollIntoView(true);", store_elements[-1])
            return store_count
        except (NoSuchElementException, TimeoutException) as e:
            self.logger.info("매장 정보를 찾을 수 없습니다.")
            self.logger.warning("매장 목록을 확인할 수 없는 에러 발생")
            self.logger.warning(e)
            return 


    def move_to_search_iframe(self):
        try:
            # 최상위 프레임으로 전환
            self.driver.switch_to.default_content()
            # iframe 존재 대기 (최대 15초)
            iframe_element = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, self.search_iframe))
            )
            self.driver.switch_to.frame(iframe_element)
            
            # 내부 요소가 완전히 로드될 때까지 대기
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

        except TimeoutException:
            self.logger.warning("❌ searchIframe 요소 찾기 실패: TimeoutException 발생")
            raise
        except Exception as e:
            self.logger.warning("❌ move_to_search_iframe 내부 오류 발생", exc_info=True)
            raise
    
    
    # 다음 페이지로 이동
    def move_to_next_page(self):

        # search Iframe으로 이동
        self.move_to_search_iframe()

        nextpage_xpath = """//a[span[contains(text(),'다음페이지')]]"""
        next_page_button = self.wait.until(
            EC.presence_of_element_located((By.XPATH, nextpage_xpath)))

        # 다음페이지 존재 여부 확인
        # 존재하지 않으면 별도의 작업없이 False 반환
        # 존재하면 다음페이지 클릭 후 True 반환
        aria_disabled = next_page_button.get_attribute("aria-disabled")
        if aria_disabled == "true":
            self.logger.info("마지막 페이지")
            return False
        else:
            self.logger.info("다음 페이지로 이동")
            next_page_button.click()
            time.sleep(2)
            return True


    # 인기메뉴 탭 크롤링
    def crawl_popular_menu(self):

        self.logger.info("인기메뉴 탭 크롤링 중...")
        self.click_filter_button()

        # 메뉴 종류 긁어오기
        menu_list_xpath = """//div[@id='modal-root']//div[@id='_popup_menu']/following-sibling::div//span/a"""
        self.wait.until(
            EC.element_to_be_clickable((By.XPATH, menu_list_xpath)))
        elements = self.driver.find_elements(By.XPATH, menu_list_xpath)
        menu_list = [element.text for element in elements].copy()

        self.logger.info(f"총 {len(menu_list)}개의 메뉴 카테고리 확인")

        # 각 메뉴에 대해 크롤링 진행
        for index, menu in enumerate(menu_list):
            if index != 0:
                self.click_filter_button()

            # 해당 메뉴를 클릭했는데, 결과가 0건인 경우, 다음 메뉴로 이어서 진행
            if self.click_menu_button(menu) == False:
                self.logger.info(f"{menu} 메뉴 검색 결과 0건임으로 해당 메뉴 생략")
                continue
            time.sleep(2)

            self.logger.info(f"[{index}/{len(menu_list)}] {menu} 메뉴 크롤링 중...")
            self.crawling_all_pages()


    # searchIframe의 필터 버튼 클릭
    def click_filter_button(self):
        time.sleep(1)
        self.move_to_search_iframe()
        # "더보기" 버튼 클릭
        filter_xpath = """//a[span[contains(text(),'전체필터')]]"""
        filtter_button = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, filter_xpath)))
        self.driver.execute_script("arguments[0].click()", filtter_button)


    # 메뉴 클릭하고 "결과보기" 버튼 클릭
    def click_menu_button(self, menu_text):
        self.logger.info("="*15 + f"{menu_text} 크롤링" + "="*15)
        menu_xpath = f"""//div[@id='modal-root']//div[@id='_popup_menu']/following-sibling::div//span/a[text()='{
            menu_text}']"""
        menu_item = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, menu_xpath)))

        self.driver.execute_script("arguments[0].click()", menu_item)
        time.sleep(0.5)

        # "결과보기" 버튼 클릭
        submit_xpath = f"""//div[@id='modal-root']//a[contains(text(), '결과보기 ')]"""
        submit_button = self.driver.find_element(By.XPATH, submit_xpath)
        # 검색 결과 0건일 경우, aria-disabled = 'true'임
        # 0건이 아닌 경우 True 반환
        if submit_button.get_attribute('aria-disabled') == 'false':
            self.driver.execute_script("arguments[0].click()", submit_button)
            return True
        else:
            return False
 
    def clean_firefox_cache(self):
        """Firefox 관련 캐시 파일 삭제"""
        try:
            self.logger.info("🧹 /tmp 내 Firefox 캐시 파일 정리")
            subprocess.run("rm -rf /tmp/rust_mozprofile* /tmp/Temp-*profile /tmp/geckodriver*", shell=True, check=True)
        except Exception:
            self.logger.warning("❌ Firefox 캐시 정리 실패", exc_info=True)

    def go_to_main_page(self):
        """네이버 지도 메인으로 이동해서 검색 상태를 초기화"""
        self.driver.get('https://map.naver.com/')
        time.sleep(2)  # 페이지 로딩 대기
        self.move_to_default_content()
        