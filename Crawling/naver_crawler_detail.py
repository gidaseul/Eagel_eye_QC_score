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
import time

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
    columns = ['naver_id','search_word','name','category', 'new_store', 'instagram_link', 'instagram_post', 'instagram_follower',
               'visitor_review_count', 'blog_review_count', 'review_category','theme_mood','theme_topic','theme_purpose', 'distance_from_subway', 'distance_from_subway_origin', 'on_tv',
               'parking_available', 'seoul_michelin', 'age-2030', 'gender-balance', 'gender_male', 'gender_female' ,'running_well', 'address', 'phone',
               'gps_latitude', 'gps_longitude','naver_url','menu_list','review_info']  
    
    def __init__(self, output_base_dir: str = None, headless: bool = True, thread_id=None, existing_naver_ids: set = None):
        self.headless = headless
        self.thread_id = thread_id
        self.search_word = "" # [신규] 검색어 저장을 위한 변수
    
        #logger 먼저 정의
        # logger 정의 (기존과 동일)
        self.logger = logging.getLogger(f"StoreCrawler_Thread_{thread_id or 0}")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(f"[%(asctime)s][Thread {thread_id or 0}] %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)


        # 나머지 초기화
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_base_dir = output_base_dir if output_base_dir else os.path.join(current_dir, 'result')
        os.makedirs(self.output_base_dir, exist_ok=True)  #디렉토리 생성 보장
        self.data = pd.DataFrame(columns=StoreCrawler.columns)
        self.user_agent_index = random.randint(0, len(USER_AGENTS) - 1)
        self.driver = self.init_driver()
        
        # [신규] 중복 방지를 위한 existing_naver_ids 세트 초기화
        self.existing_naver_ids = existing_naver_ids if existing_naver_ids is not None else set()

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

    # [신규] 새로운 최상위 실행 메소드
    def run_crawl(self, search_query: str, latitude: float = None, longitude: float = None):
        """
        입력 파라미터에 따라 크롤링 전체 과정을 조율하고 실행합니다.
        """
        self.search_word = search_query
        self.logger.info(f"크롤링 작업 시작. 검색어: '{search_query}', 좌표: ({latitude}, {longitude})")

        try:
            # 1. 위도/경도 유무에 따른 시작 페이지 분기
            if latitude and longitude:
                self.logger.info(f"좌표 기반 검색 시작: {latitude}, {longitude}")
                url = f"https://map.naver.com/p?c=17.00,{longitude},{latitude},0,0,0,dh"
                self.driver.get(url)
            else:
                self.logger.info("키워드 기반 검색 시작")
                self.driver.get('https://map.naver.com/')
            time.sleep(2) # 페이지 로딩 대기

            # 2. 키워드 검색 실행
            search_result_type = self.search_keyword()

            # 3. 검색 결과에 따라 분기 처리
            if search_result_type == 'search':
                self.logger.info("검색 결과: 목록 페이지. 전체 목록 크롤링을 시작합니다.")
                self.crawl_all_results_in_list()
            elif search_result_type == 'entry':
                self.logger.info("검색 결과: 단일 상세 페이지. 해당 가게 정보를 크롤링합니다.")
                self.init_dictionary()
                self.get_store_details()
            else: # 'none', 'error', 'unknown'
                self.logger.warning("검색 결과가 없거나 오류가 발생하여 크롤링을 종료합니다.")

        except Exception as e:
            self.logger.error(f"크롤링 실행 중 심각한 오류 발생: {e}", exc_info=True)
        finally:
            self.quit()
            self.logger.info(f"크롤링 작업 완료. 총 {len(self.data)}개 데이터 수집.")
            return self.data

    # [신규] 검색 목록의 모든 가게를 크롤링하는 메소드
    # naver_crawler_detail.py의 crawl_all_results_in_list 메소드 (최종 완성본)
    def crawl_all_results_in_list(self):
        page = 1
        while True:
            try:
                self.move_to_search_iframe()
                self.logger.info(f"===== {page} 페이지 크롤링 시작 =====")
                self.scroll_to_end()
                time.sleep(1.5)
                store_elements_xpath = "//*[@id='_pcmap_list_scroll_container']/ul/li"
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, store_elements_xpath)))
                store_elements = self.driver.find_elements(By.XPATH, store_elements_xpath)
                self.logger.info(f"{page} 페이지에서 {len(store_elements)}개의 가게를 찾았습니다.")
            except Exception as e:
                self.logger.warning(f"{page} 페이지 로딩 중 오류: {e}")
                break

            for i in range(len(store_elements)):
                store_name_for_log = "[이름 확인 불가]"
                try:
                    # [중요] StaleElementReferenceException을 원천적으로 방지하기 위해
                    # 루프가 돌 때마다 목록과 현재 처리할 요소를 새로고침합니다.
                    self.move_to_search_iframe()
                    current_elements = self.driver.find_elements(By.XPATH, store_elements_xpath)
                    if i >= len(current_elements): break
                    
                    target_li = current_elements[i]

                    # --- [가장 안정적인 3단계 클릭 전략] ---

                    # 1단계: 정확한 클릭 대상 특정
                    # 'place_bluelink' 클래스를 가진 <a> 태그를 대상으로 지정합니다.
                    click_target_selector = "a.place_bluelink"
                    target_element = target_li.find_element(By.CSS_SELECTOR, click_target_selector)
                    store_name_for_log = target_element.text.split("\n")[0]
                    self.logger.info(f"--- {page} 페이지 {i+1}/{len(store_elements)} 번째 '{store_name_for_log}' 처리 시작 ---")
                    
                    # 2단계: 요소의 가시성 및 '클릭 가능' 상태 확보
                    #   a. 요소를 화면에 보이도록 스크롤합니다.
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", target_element)
                    #   b. 요소가 다른 것에 가려지지 않고 클릭 가능한 상태가 될 때까지 명시적으로 기다립니다.
                    clickable_element = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, f"li:nth-child({i+1}) {click_target_selector}"))
                    )

                    # 3단계: 이벤트 실행 및 결과(iframe) 대기
                    self.driver.execute_script("arguments[0].click();", clickable_element)
                    
                    # 클릭 후, 최상위 프레임으로 나와 entryIframe이 나타날 때까지 기다립니다.
                    self.driver.switch_to.default_content()
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.ID, self.entry_iframe))
                    )
                    self.logger.info("entryIframe 로딩 확인 완료.")

                    # 모든 조건이 만족되었으므로, 상세 정보 수집을 진행합니다.
                    self.init_dictionary()
                    self.get_store_details()

                except Exception as e:
                    self.logger.error(f"'{store_name_for_log}' 가게 처리 중 오류 발생. 건너뜁니다. 에러: {e}", exc_info=True)
                    # 문제가 발생해도 다음 가게로 계속 진행
                    continue
            
            if not self.move_to_next_page():
                break
            page += 1


    def init_driver(self):
        ua = random.choice(USER_AGENTS)
        self.logger.info(f"[UA] 현재 user-agent: {ua}")
        try:
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
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument(f'user-agent={ua}')

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
            return driver
        except Exception as e:
            self.logger.error("❌ WebDriver 초기화 실패", exc_info=True)
            return None  # ❗ 실패 시 반드시 None 반환

    def search_keyword(self):  #어떤 Frame으로 넘어가야 하는지 확인하기
        self.logger.info(f"{self.search_word} 검색어 입력 중...")
        self.move_to_default_content()
        time.sleep(1)
        try:
            search_box = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".input_search")))
            self.driver.execute_script("arguments[0].value = '';", search_box)
            time.sleep(0.5)
            search_box.send_keys(self.search_word)
            time.sleep(1)
            search_box.send_keys(Keys.RETURN)
            time.sleep(4.5) 

            iframe_elements = self.driver.find_elements(By.TAG_NAME, "iframe")
            iframe_ids = [iframe.get_attribute("id") for iframe in iframe_elements]

            if "entryIframe" in iframe_ids:
                return "entry"
            elif "searchIframe" in iframe_ids:
                return "search"
            else:
                self.driver.switch_to.frame("searchIframe")
                time.sleep(1)
                try:
                    no_result_elem = self.driver.find_element(By.CLASS_NAME, "FYvSc") # 검색 결과가 없을 때 나타나는 클래스
                    self.logger.info(f"검색 결과 없음: {no_result_elem.text.strip()}")
                    if "조건에 맞는 업체가 없습니다" in no_result_elem.text.strip():
                        return "none"
                except:
                    return "search" # 업체없음 문구가 없어도 searchIframe은 있으므로
            return "unknown"
        except Exception as e:
            self.logger.warning(f"❌ 검색어 입력 중 오류 발생: {e}")
            return "error"
        
    # Iframe 내부에 있을 때, 가장 상위의 frame으로 이동
    def move_to_default_content(self):
        self.driver.switch_to.default_content()

    # 한 매장에 대한 크롤링을 마치고, 그 다음 매장을 크롤링 하기 위해 실행
    def init_dictionary(self):
        self.store_dict = {
            "search_word": self.search_word,  # [신규] 검색어 저장
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
            "distance_from_subway_origin":None, 
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
            "naver_url": None,
            "review_info": [],
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
        try:
            self.driver.switch_to.default_content()
            iframe_element = self.wait.until(EC.presence_of_element_located((By.ID, self.entry_iframe)))
            self.driver.switch_to.frame(iframe_element)
            # iframe 내부 body가 로드될 때까지 추가 대기
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except TimeoutException:
            self.logger.error("❌ entryIframe으로 전환 실패 (Timeout)")
            raise


    # 탭 이동 후 직접 time sleep을 사용해 대시한다
    def move_to_tab(self, tab_name):
        tab_xpath = f"""//a[@role='tab' and .//span[text()='{tab_name}']]"""
        # tab_element = self.driver.find_element(By.XPATH, tab_xpath)
        tab_element = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, tab_xpath))
        )        
        self.driver.execute_script("arguments[0].click()", tab_element)
        time.sleep(2)


    # [수정] naver_id 인자를 제거하고, 내부 로직을 '현재 로드된 페이지' 기준으로 변경
    def get_store_details(self):
        try:
            self.logger.info("매장 상세 정보 수집 시작...")
            
            # [수정 1] iframe에 들어가기 전에 URL과 ID를 먼저 확인
            try:
                self.move_to_default_content()
                iframe_el = WebDriverWait(self.driver, 15).until(
                    lambda d: d.find_element(By.ID, self.entry_iframe)
                )
                ifram_src = iframe_el.get_attribute("src")
                self.store_dict["naver_url"] = ifram_src

                match = re.search(r'/place/(\d+)', ifram_src)
                self.store_dict["naver_id"] = int(match.group(1)) if match else None
            except TimeoutException:
                self.logger.warning("❌ entryIframe 로딩 실패 (Timeout)")
                return False
            except Exception as e:
                self.logger.warning(f"❌ entryIframe 전환 실패: {e}")
                self.sotre_dict["naver_url"] = None
                self.store_dict["naver_id"] = None
            
            # [수정 2] iframe 안으로 딱 한 번만 진입합니다.
            try:
                self.driver.switch_to.frame(iframe_el)
                self.logger.info("entryIframe으로 전환 완료.")
            except Exception as e:
                self.logger.warning(f"❌ entryIframe으로 전환 실패: {e}")
                return False
            

            try:
                # 1. __APOLLO_STATE__가 로드될 때까지 명시적으로 대기합니다.
                WebDriverWait(self.driver, 10).until(
                    lambda d: d.execute_script("return window.__APOLLO_STATE__;")
                )
                
                # 2. execute_script로 __APOLLO_STATE__ 객체 자체를 가져옵니다.
                apollo_state = self.driver.execute_script("return window.__APOLLO_STATE__;")
                
                is_gps_found = False
                # 3. 파이썬 딕셔너리로 순회하며 'coordinate' 키를 탐색합니다.
                for key, value in apollo_state.items():
                    if isinstance(value, dict) and "coordinate" in value and value["coordinate"] is not None:
                        # [수정] 'Place:'로 시작한다는 가정을 버리고, 'coordinate' 키 존재 여부만 확인
                        coords = value["coordinate"]
                        if coords.get("x") and coords.get("y"):
                            self.store_dict["gps_longitude"] = float(coords.get("x"))
                            self.store_dict["gps_latitude"] = float(coords.get("y"))
                            self.logger.info(f"✅ GPS 좌표 추출 성공: 키 '{key}'에서 찾음 -> ({self.store_dict['gps_latitude']}, {self.store_dict['gps_longitude']})")
                            is_gps_found = True
                            break  # 좌표를 찾았으면 더 이상 탐색할 필요가 없습니다.
                
                if not is_gps_found:
                    self.logger.warning("❌ __APOLLO_STATE__ 데이터 안에서 'coordinate' 정보를 찾지 못했습니다.")
                    # [진단 코드] 어떤 키들이 있는지 확인하기 위해 모든 키 목록을 출력합니다.
                    self.logger.warning(f"전체 APOLLO_STATE 키 목록: {list(apollo_state.keys())}")

            except Exception as e:
                self.logger.error(f"❌ GPS 좌표 추출 중 심각한 오류 발생: {e}")
                self.store_dict["gps_latitude"] = None
                self.store_dict["gps_longitude"] = None

            # 3. 이름, 카테고리, 새로오픈 여부
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

            except NoSuchElementException as e:
                self.store_dict["address"] = None

            except Exception as e:
                self.logger.warning(f"❌ 주소 또는 address 추출 실패: {e}")
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
                    
                # '더보기' 버튼 클릭 (존재 여부 파악이 우선적으로 필요함. 없는 경우도 존재하기 때문)
                try:
                    # 1. '더보기' 버튼이 존재하는지 먼저 빠르게 확인합니다.
                    more_button_selector = ".//div[contains(@class, 'NSTUp')]//span[contains(text(), '더보기')]"
                    more_buttons = datalab_section.find_elements(By.XPATH, more_button_selector)
                    if more_buttons:
                        self.logger.info("'더보기' 버튼이 존재합니다. 클릭을 시도합니다.")
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
                    else:
                        # 3. 버튼이 존재하지 않으면, 로그만 남기고 넘어갑니다.
                        self.logger.info("'더보기' 버튼이 없어 확장 과정을 건너뜁니다.")

                except Exception as e:
                    self.logger.warning(f"❌ '더보기' 버튼 처리 중 오류 발생: {e}")


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
                    self.logger.info(f"메뉴 개수: {len(li_elements)}")

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
            if self.store_dict.get('instagram_link'):
                main_tab = self.driver.window_handles[0]
                insta_tab = self.driver.window_handles[1]

                try:
                    self.logger.info("인스타그램 정보 수집 시작...")
                    instagram_embed_url = self.store_dict['instagram_link'] + "/embed"

                    # 2번째 탭(인스타그램)으로 이동
                    self.driver.switch_to.window(insta_tab)
                    self.driver.get(instagram_embed_url)

                    name_xpath = """/html/body/div/div/div/div/div/div/div/div/div[1]/div[2]/div[1]/a/div"""
                    self.wait_medium.until(EC.presence_of_element_located(
                        (By.XPATH, name_xpath)))

                    #  insta follower , post 위치 변경 반영
                    follower_xpath = """/html/body/div/div/div/div/div/div/div/div/div[1]/div[2]/div[3]/span/div[1]/span/span"""
                    post_xpath = """/html/body/div/div/div/div/div/div/div/div/div[1]/div[2]/div[3]/span/div[2]/span/span"""

                    follower_elem = self.driver.find_element(By.XPATH, follower_xpath)
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
                
                finally:
                    # [중요 사항] 어떠한 일이 있어도 반드시 원래 탭으로 복귀하게 하기
                    self.logger.info("네이버 지도 탭으로 복귀합니다.")
                    self.driver.switch_to.window(main_tab)
                    # [중요 사항] 네이버 지도의 entry iframe으로 다시 전환
                    self.move_to_entry_iframe()   

            
            # 5. 모든 정보 수집 후 데이터프레임에 추가
            self.insert_into_dataframe()
            self.logger.info(f"'{self.store_dict.get('name', 'N/A')}' 상세 정보 수집 완료.")
        except Exception as e:
            self.logger.error(f"get_store_details 실행 중 치명적 오류: {e}", exc_info=True)


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
        if not self.store_dict or not self.store_dict.get('name'):
            self.logger.warning("Store_dict가 비어있거나 이름이 없어 데이터프레임에 추가하지 않음")
            return
        
        try:
            ordered_dict = {col: self.store_dict.get(col) for col in self.columns}
            new_data = pd.DataFrame([ordered_dict])
            self.data = pd.concat([self.data, new_data], ignore_index=True)
            self.logger.info(f"'{self.store_dict['name']}' 정보 추가 완료. 현재 수집 개수: {len(self.data)}")
        except Exception as e:
            self.logger.warning(f"❌ DataFrame에 데이터 추가 실패: {e}")


    def scroll_to_end(self):
        try:
            scroll_container_selector = "#_pcmap_list_scroll_container"
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, scroll_container_selector)))
            
            last_height = self.driver.execute_script(f"return document.querySelector('{scroll_container_selector}').scrollHeight")
            while True:
                self.driver.execute_script(f"document.querySelector('{scroll_container_selector}').scrollTo(0, {last_height});")
                time.sleep(1.5)
                new_height = self.driver.execute_script(f"return document.querySelector('{scroll_container_selector}').scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception:
            self.logger.info("스크롤할 매장 목록이 없습니다.")

    def move_to_search_iframe(self):
        try:
            self.driver.switch_to.default_content()
            iframe_element = self.wait.until(EC.presence_of_element_located((By.ID, self.search_iframe)))
            self.driver.switch_to.frame(iframe_element)
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except TimeoutException:
            self.logger.error("❌ searchIframe으로 전환 실패 (Timeout)")
            raise

    def move_to_next_page(self):
        try:
            self.move_to_search_iframe()
            next_page_button = self.driver.find_element(By.XPATH, "//a[contains(@class, 'mBN2s') and span[text()='다음페이지']]")
            if next_page_button.get_attribute("aria-disabled") == "true":
                return False
            else:
                next_page_button.click()
                time.sleep(2)
                return True
        except NoSuchElementException:
            self.logger.info("다음 페이지 버튼이 없어 마지막 페이지로 간주합니다.")
            return False

    def quit(self):
        if self.driver:
            self.driver.quit()
            self.driver = None
