import logging
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def extract_store_info(driver, iframe_element, index):
    """
    네이버 지도 iframe에서 GPS 좌표 추출

    Args:
        driver: Selenium WebDriver
        iframe_element: iframe 요소
        index: 매장 인덱스

    Returns:
        dict: {'latitude': 위도, 'longitude': 경도} 또는 None
    """
    try:
        # Step 1: iframe 전환
        driver.switch_to.frame(iframe_element)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, 'script'))  # script 태그 로딩 완료 대기
        )

        # Step 2: script 태그 수집
        script_elements = driver.find_elements(By.TAG_NAME, 'script')

        # Step 3: 첫 번째 non-empty script 태그 추출
        script_content = None
        for script in script_elements:
            content = script.get_attribute('innerHTML')
            if content and content.strip():  # 내용이 있는 경우
                script_content = content
                break
        else:
            logging.warning(f"[{index}] Script 태그 데이터 없음.")
            return None

        # Step 4: 좌표 추출 (정규식 분석)
        pattern = r'{"__typename":"Coordinate","x":"([0-9.]+)","y":"([0-9.]+)","mapZoomLevel":([0-9]+)}'
        matches = re.findall(pattern, script_content)

        if matches:
            # 첫 번째 좌표 정보 추출
            x, y, _ = matches[0]
            latitude = float(y)
            longitude = float(x)
            logging.warning(f"[{latitude}]latitude")

            return {"latitude": latitude, "longitude": longitude}
        
        else:
            logging.warning(f"[{index}] GPS 좌표 추출 실패.")
            return None

    except Exception as e:
        logging.warning(f"[{index}] GPS 추출 중 에러: {e}")
        return None