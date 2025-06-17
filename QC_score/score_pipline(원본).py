import json
import os
import google.generativeai as genai
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv
from typing import List, Dict, Optional
import sys
import datetime
import pandas as pd
from shapely.geometry import Point, Polygon
from shapely import wkt

# ----------------------------------------------------------------------
# 1. 파일 경로 설정 (★★★★ 이 부분을 당신의 실제 환경에 맞게 수정해야 합니다 ★★★★)
#    - config.env와 JSON 데이터 파일들이 위치한 디렉토리의 절대 경로를 정확히 입력하세요.
# ----------------------------------------------------------------------

# config.env 파일의 절대 경로
CONFIG_ENV_PATH = ".config.env"

# JSON 데이터 파일들이 위치한 디렉토리의 절대 경로
JSON_DATA_DIR = "QC_score"

# Polygon CSV 파일 경로 추가 (JSON_DATA_DIR과 같은 디렉토리로 가정)
HOTSPOT_POLYGON_CSV = os.path.join(JSON_DATA_DIR, "seoul_hotspots_polygons.csv")
CAMPUS_POLYGON_CSV = os.path.join(JSON_DATA_DIR, "campus_polygons.csv")


# .env 파일에서 환경 변수를 로드합니다.
load_dotenv(dotenv_path=CONFIG_ENV_PATH)

# 환경 변수에서 API 키를 로드
API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    print(f"오류: 환경 변수 'GOOGLE_API_KEY'가 설정되지 않았습니다. '{CONFIG_ENV_PATH}' 파일을 확인해주세요.")
    sys.exit(1)

# Gemini 클라이언트 초기화
genai.configure(api_key=API_KEY)

# ----------------------------------------------------------------------
# API 키 유효성 검증 함수 (옵션이지만 추천)
# ----------------------------------------------------------------------
def _check_api_key_validity():
    """
    설정된 Gemini API 키의 유효성을 간단한 API 호출로 검증합니다.
    """
    try:
        list(genai.list_models())
        print("API 키가 성공적으로 검증되었습니다. Gemini API에 접근할 수 있습니다.")
        return True
    except Exception as e:
        print(f"오류: API 키 검증 실패. API 키가 유효하지 않거나 네트워크 문제가 있을 수 있습니다.")
        print(f"상세 오류: {e}")
        print("API 키를 다시 확인하거나, Google AI Studio에서 새 키를 발급받아보세요.")
        return False

# API 키 유효성 검증 실행 (스크립트 시작 시 한 번만)
if not _check_api_key_validity():
    sys.exit(1)


# 1. 매핑 및 예시 JSON 파일 로드 함수
def load_json_data(full_file_path: str):
    """지정된 절대 경로의 JSON 파일을 읽어 Python 객체로 반환합니다."""
    try:
        with open(full_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"오류: 파일 '{full_file_path}'를 찾을 수 없습니다. 파일 경로를 다시 확인해주세요.")
        return None
    except json.JSONDecodeError:
        print(f"오류: 파일 '{full_file_path}'의 JSON 형식이 올바르지 않습니다. 파일 내용을 확인해주세요.")
        return None
    except Exception as e:
        print(f"오류: 파일 '{full_file_path}' 로드 중 예상치 못한 오류 발생: {e}")
        return None

# Pydantic 모델 정의
class StoreCategoryResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    naver_id: str = Field(description="입력된 매장 정보의 고유 ID")
    name: str = Field(description="입력된 매장 이름")
    대분류: str = Field(description="매장 정보에서 추론된 대분류 카테고리")
    중분류: str = Field(description="매장 정보에서 추론된 중분류 카테고리")
    소분류: str = Field(description="매장 정보에서 추론된 소분류 카테고리 (해당 없으면 빈 문자열)")
    메뉴_라벨: str = Field(description="매장 정보에서 추론된 라벨 (해당 없으면 빈 문자열)")
    메뉴_점수: str = Field(description="추론된 라벨에 해당하는 점수 (해당 없으면 빈 문자열)")
    메뉴_추론근거: str = Field(description="LLM이 해당 카테고리 및 라벨을 선택한 상세한 추론 과정")


# 2. System Prompt 정의
SYSTEM_PROMPT = """
당신은 대한민국 내 음식점 및 상점 카테고리 분류 전문가입니다.
주어진 매장 정보를 바탕으로 가장 정확한 '대분류', '중분류', '소분류', '라벨' 그리고 '점수'를 매핑해야 합니다.
매핑할 '라벨'이 제공된 '라벨 매핑 정보'에 존재한다면 반드시 해당 '점수'를 부여하세요.
**만약 '라벨'이 존재하지 않아 '점수'를 부여할 수 없다면, `메뉴_라벨`과 `메뉴_점수` 필드를 반드시 빈 문자열('')로 표시해야 합니다.**
**마찬가지로, 해당되는 '소분류'를 찾을 수 없다면 `소분류` 필드를 빈 문자열('')로 표시해야 합니다.**
응답은 반드시 JSON 형식으로만 제공해야 합니다.
각 필드(`대분류`, `중분류`, `소분류`, `메뉴_라벨`, `메뉴_점수`, `메뉴_추론근거`)는 문자열 타입이어야 합니다.
'메뉴_추론근거' 필드에 추론 과정을 상세히 설명해야 합니다. **라벨이 없는 경우에도 '메뉴_추론근거'는 반드시 포함되어야 하며, 라벨이 없는 이유를 설명하세요.**
카테고리 및 라벨 매핑 정보가 주어지지 않았다면, 주어진 텍스트 내용만을 기반으로 최선의 추론을 하되, 정보 부족을 '메뉴_추론근거'에 명시하세요.
""".strip()

# 3. Few-Shot Examples 정의
FEW_SHOT_EXAMPLES = """
## Few-Shot Examples (예시 학습)

### 2.1. Example Category Mappings (카테고리 매핑 예시)
다음은 음식 카테고리 매핑 정보의 예시입니다:
대분류: 음식점, 중분류: 한식, 소분류: 한정식, 목록: ["한정식"]
대분류: 음식점, 중분류: 일식, 소분류: 초밥,롤, 목록: ["초밥,롤"]
대분류: 음식점, 중분류: 양식, 소분류: 햄버거, 목록: ["햄버거"]
대분류: 음식점, 중분류: 중식, 소분류: 마라탕, 목록: ["마라탕"]
대분류: 음식점, 중분류: 술집, 소분류: 바(BAR), 목록: ["바(BAR)", "라운지바", "루프톱바"]

### 2.2. Example Score Mappings (점수 매핑 예시) - 다음과 같은 예시를 보면서 라벨을 부여하는 예시
다음은 특정 음식점 라벨에 대한 정보의 예시입니다. 각 라벨에는 점수가 부여되어 있습니다 (5가 가장 높음, 1이 가장 낮음):
라벨: 수제 햄버거 전문점, 점수: 5, 매핑 카테고리: 햄버거, 관련 키워드: 햄버거
라벨: 샤브샤브, 점수: 5, 매핑 카테고리: 샤브샤브, 관련 키워드:
라벨: 스테이크 전문점, 점수: 5, 매핑 카테고리: 스테이크,립 관련 키워드: 스테이크
라벨: 갈비 무한 리필, 점수: 5, 매핑 카테고리: 고기뷔페,육류,고기요리, 관련 키워드: 무한리필
라벨: 오마카세, 점수: 5, 매핑 카테고리: 일식당, 관련 키워드: 오마카세
라벨: 마라탕, 점수: 4, 매핑 카테고리: 중식당,마라탕,양꼬치, 관련 키워드: 마라탕
라벨: 프랑스 코스 요리, 점수: 4, 매핑 카테고리: 프랑스음식, 관련 키워드: 코스요리,레스토랑
라벨: 경양식 돈까스, 점수: 3, 매핑 카테고리: 돈가스, 관련 키워드: 경양식,돈까스,돈가스
라벨: 한상차림식 한식백반, 점수: 2, 매핑 카테고리: 찌개,전골,한식,생선구이,기사식당, 관련 키워드: 백반,정식
라벨: 동네 중국집, 점수: 1, 매핑 카테고리: 중식당, 관련 키워드: 자장면,짜장면,탕수육,짬뽕
라벨 : 분식, 점수: 1, 매핑 카테고리: 분식, 관련 키워드: 떡볶이,김밥,순대,튀김
라벨 : 꼬막비빔밥, 점수: 4, 매핑 카테고리: 한식, 관련 키워드: 꼬막비빔밥, 꼬막 비빔밥

### 2.3. Example Complex Cases (결정 경계 및 복합 추론 예시 - 기존 예시)
이 섹션은 LLM이 가장 까다로운 케이스들을 학습하는 데 사용됩니다. 각 입력 데이터 컬럼(name, category, theme_topic, theme_purpose, menu_list, review_info)이 분류에 어떻게 기여하는지 명확히 보여줍니다.

### 라벨이 있는 경우

텍스트:
매장 이름: 전국구식당
기존 카테고리: 한식
테마 토픽: 가정식
테마 목적: 혼밥
메뉴 정보: 김치찌개 (얼큰한 맛), 된장찌개 (구수한 맛), 제육볶음 (매콤한 맛)
리뷰 정보: 집밥 같은 느낌, 반찬 푸짐해서 좋아요.

응답:
```json
{{
"naver_id" : "example_naver_id_1234",
"name" : "전국구식당",
  "대분류": "음식점",
  "중분류": "한식",
  "소분류": "백반,가정식",
  "메뉴_라벨": "한상차림식 한식백반",
  "메뉴_점수": "2",
  "메뉴_추론근거": "기존 category는 '한식'이지만, theme_topic '가정식', menu_list의 '김치찌개, 된장찌개', review_info '집밥 같은 느낌'을 종합할 때 '백반,가정식' 소분류에 해당하며, 특히 '한상차림식 한식백반' 라벨이 적합합니다. score_mapping에 따라 점수는 '2'입니다."
}}

### 라벨이 없는 경우

텍스트:
매장 이름: 예쁜카페
기존 카테고리: 카페
테마 토픽: 뷰맛집
테마 목적: 데이트
메뉴 정보: 아메리카노, 카페라떼, 티라미수 케이크
리뷰 상세 정보: 창덕궁 뷰가 너무 예뻐요, 디저트도 맛있어요.

응답:
```json
{{
  "naver_id" : "example_naver_id_cafe",
  "name" : "예쁜카페",
  "대분류": "음식점",
  "중분류": "카페",
  "소분류": "",
  "메뉴_라벨": "",
  "메뉴_점수": "",
  "메뉴_추론근거": "기존 카테고리가 '카페'이고, 메뉴와 리뷰에서 커피 및 디저트 관련 정보가 확인됩니다. 제공된 라벨 매핑 정보에는 해당되는 특정 카페 라벨이 없어 빈 문자열로 처리했습니다."
}}
""".strip()

# store_crawling_20250523.json의 데이터를 동적으로 프롬프트에 추가할 함수
def format_test_data_as_examples(test_data_list: List[Dict]) -> str:
    formatted_examples = ""
    if test_data_list is None:
        return ""

    for idx, data_item in enumerate(test_data_list):
        naver_id = data_item.get("naver_id", "")
        name = data_item.get("name", "")
        category = data_item.get("category", "")

        review_category = ""
        review_category_raw = data_item.get("review_category")
        if isinstance(review_category_raw, str):
            try: review_category_dict = json.loads(review_category_raw.replace("'", "\""))
            except json.JSONDecodeError: review_category_dict = {}
            review_category = ", ".join([f"{k}: {v}" for k, v in review_category_dict.items()])
        elif isinstance(review_category_raw, dict):
            review_category = ", ".join([f"{k}: {v}" for k, v in review_category_raw.items()])

        theme_mood = ""
        theme_mood_raw = data_item.get("theme_mood")
        if isinstance(theme_mood_raw, str):
            try: theme_mood = ", ".join(json.loads(theme_mood_raw.replace("'", "\"")))
            except json.JSONDecodeError: theme_mood = theme_mood_raw
        elif isinstance(theme_mood_raw, list): theme_mood = ", ".join(theme_mood_raw)

        theme_topic = ""
        theme_topic_raw = data_item.get("theme_topic")
        if isinstance(theme_topic_raw, str):
            try: theme_topic = ", ".join(json.loads(theme_topic_raw.replace("'", "\"")))
            except json.JSONDecodeError: theme_topic = theme_topic_raw
        elif isinstance(theme_topic_raw, list): theme_topic = ", ".join(theme_topic_raw)

        theme_purpose = ""
        theme_purpose_raw = data_item.get("theme_purpose")
        if isinstance(theme_purpose_raw, str):
            try: theme_purpose = ", ".join(json.loads(theme_purpose_raw.replace("'", "\"")))
            except json.JSONDecodeError: theme_purpose = theme_purpose_raw
        elif isinstance(theme_purpose_raw, list): theme_purpose = ", ".join(theme_purpose_raw)

        menu_names = [m.get("name", "") for m in data_item.get("menu_list", []) if m and m.get("name")]
        menu_str = ", ".join(menu_names) if menu_names else "없음"

        review_comments = [r.get("comment", "") for r in data_item.get("review_info", []) if r and r.get("comment")]
        review_info_str = " ".join(review_comments) if review_comments else "없음"
        
        formatted_examples += f"""
### 추가 매장 정보 예시 {idx + 1}:
naver_id: {naver_id}
매장 이름: {name}
기존 카테고리: {category}
리뷰 카테고리 요약: {review_category}
테마 분위기: {theme_mood}
테마 토픽: {theme_topic}
테마 목적: {theme_purpose}
메뉴 정보: {menu_str}
리뷰 상세 정보: {review_info_str}
"""
    return formatted_examples.strip()


# 4. 관련 매핑 데이터 및 추가 예시 로드 및 문자열화 (함수 밖에서 전역으로 로드)
CATEGORY_MAPPING = load_json_data(os.path.join(JSON_DATA_DIR, 'category_mapping.json'))
SCORE_MAPPING = load_json_data(os.path.join(JSON_DATA_DIR, 'score_mapping_54321.json'))

if CATEGORY_MAPPING is None:
    print(f"오류: '{os.path.join(JSON_DATA_DIR, 'category_mapping.json')}' 파일을 로드할 수 없습니다. 파일 경로를 확인하거나 해당 파일이 존재하는지 확인해주세요.")
    sys.exit(1)
if SCORE_MAPPING is None:
    print(f"오류: '{os.path.join(JSON_DATA_DIR, 'score_mapping_54321.json')}' 파일을 로드할 수 없습니다. 파일 경로를 확인하거나 해당 파일이 존재하는지 확인해주세요.")
    sys.exit(1)

CATEGORY_MAPPING_STR = json.dumps(CATEGORY_MAPPING, ensure_ascii=False, indent=4)
SCORE_MAPPING_STR = json.dumps(SCORE_MAPPING, ensure_ascii=False, indent=4)


def generate_categorization_prompt(store_data: Dict, additional_examples_str: str) -> str:
    """
    주어진 단일 매장 데이터를 기반으로 Gemini 모델에 보낼 프롬프트 내용을 생성합니다.
    additional_examples_str 인자를 추가하여 동적으로 추가 예시를 삽입합니다.
    """
    naver_id = str(store_data.get("naver_id", ""))
    name = store_data.get("name", "")
    category = store_data.get("category", "")

    review_category_raw = store_data.get("review_category", "{}")
    review_category = ""
    if isinstance(review_category_raw, str):
        try: review_category_dict = json.loads(review_category_raw.replace("'", "\""))
        except json.JSONDecodeError: review_category_dict = {}
        review_category = ", ".join([f"{k}: {v}" for k, v in review_category_dict.items()])
    elif isinstance(review_category_raw, dict):
        review_category = ", ".join([f"{k}: {v}" for k, v in review_category_raw.items()])

    theme_mood_raw = store_data.get("theme_mood")
    theme_mood = ""
    if isinstance(theme_mood_raw, str):
        try: theme_mood = ", ".join(json.loads(theme_mood_raw.replace("'", "\"")))
        except json.JSONDecodeError: theme_mood = theme_mood_raw
    elif isinstance(theme_mood_raw, list): theme_mood = ", ".join(theme_mood_raw)

    theme_topic_raw = store_data.get("theme_topic")
    theme_topic = ""
    if isinstance(theme_topic_raw, str):
        try: theme_topic = ", ".join(json.loads(theme_topic_raw.replace("'", "\"")))
        except json.JSONDecodeError: theme_topic = theme_topic_raw
    elif isinstance(theme_topic_raw, list): theme_topic = ", ".join(theme_topic_raw)

    theme_purpose_raw = store_data.get("theme_purpose")
    theme_purpose = ""
    if isinstance(theme_purpose_raw, str):
        try: theme_purpose = ", ".join(json.loads(theme_purpose_raw.replace("'", "\"")))
        except json.JSONDecodeError: theme_purpose = theme_purpose_raw
    elif isinstance(theme_purpose_raw, list): theme_purpose = ", ".join(theme_purpose_raw)

    menu_names = [item.get("name", "") for item in store_data.get("menu_list", []) if item and item.get("name")]
    menu_str = ", ".join(menu_names) if menu_names else "없음"

    review_comments = [item.get("comment", "") for item in store_data.get("review_info", []) if item and item.get("comment")]
    review_info_str = " ".join(review_comments) if review_comments else "없음"

    # 사용자 입력 부분을 구성
    user_input_text = f"""
---
## 현재 분류할 매장 정보:
naver_id: {naver_id}
매장 이름: {name}
기존 카테고리: {category}
리뷰 카테고리 요약: {review_category}
테마 분위기: {theme_mood}
테마 토픽: {theme_topic}
테마 목적: {theme_purpose}
메뉴 정보: {menu_str}
리뷰 상세 정보: {review_info_str}
"""

    # 최종 프롬프트 구성
    full_prompt = f"""
{SYSTEM_PROMPT}

{FEW_SHOT_EXAMPLES}

### 2.4. 추가적인 실제 매장 정보 예시 (복합 추론 학습용)
이 섹션은 모델이 다양한 실제 매장 데이터의 패턴을 이해하고, 이를 바탕으로 더욱 정확한 의미론적 추론을 수행하도록 돕습니다. 다음은 그 예시 데이터입니다:
<ADDITIONAL_STORE_EXAMPLES>
{additional_examples_str}
</ADDITIONAL_STORE_EXAMPLES>

## 관련 매핑 데이터 (참고용)

### 카테고리 매핑 정보:
<CATEGORY_MAPPING_DATA>
{CATEGORY_MAPPING_STR}
</CATEGORY_MAPPING_DATA>

### 라벨별 점수 매핑 정보:
<SCORE_MAPPING_DATA>
{SCORE_MAPPING_STR}
</SCORE_MAPPING_DATA>

{user_input_text}

---
위의 지시사항, 예시, 매핑 데이터 그리고 현재 매장 정보를 바탕으로 가장 적절한 대분류, 중분류, 소분류, 라벨, 점수를 추론하고, 그 추론 근거를 포함하여 정확히 다음 JSON 스키마에 맞춰 응답하세요.
(응답할 JSON 스키마: {json.dumps(StoreCategoryResponse.model_json_schema(), ensure_ascii=False, indent=2)})
응답:
"""
    return full_prompt


def get_categorized_store_info(store_data: Dict, additional_examples_str: str) -> Optional[Dict]:
    """
    주어진 단일 매장 정보를 Gemini 모델에 보내어 카테고리 분류 결과를 받습니다.
    additional_examples_str 인자를 추가하여 동적으로 추가 예시를 전달합니다.
    """
    prompt = generate_categorization_prompt(store_data, additional_examples_str)

    try:
        model = genai.GenerativeModel(model_name="gemini-2.0-flash")
        response = model.generate_content(
            contents=prompt,
            generation_config={
                "response_mime_type" : "application/json",
            },
            request_options={"timeout": 15} # 15초 타임아웃 추가
        )
        print(f"DEBUG: Raw LLM response text:\n{response.text}")
        validated_response = StoreCategoryResponse.model_validate_json(response.text)
        return validated_response.model_dump()

    except Exception as e:
        print(f"\n--- API 호출 또는 응답 파싱 중 오류 발생 for '{store_data.get('name', 'N/A')}' (ID: {store_data.get('naver_id', 'N/A')}) ---")
        if 'response' in locals() and hasattr(response, 'text'):
            print(f"--- FAILED RAW TEXT ---\n{response.text}\n-----------------------")
        print(f"오류: {e}")
        return None

def load_polygons_from_df(file_path: str, name_col: str, polygon_col: str) -> Dict[str, Polygon]:
    polygons = {}
    try:
        df = pd.read_csv(file_path)
        if name_col not in df.columns or polygon_col not in df.columns:
            print(f"경고: '{file_path}'에 '{name_col}' 또는 '{polygon_col}' 컬럼이 없습니다.")
            return {}

        for _, row in df.iterrows():
            try:
                name = row[name_col]
                poly = wkt.loads(row[polygon_col])
                polygons[name] = poly
            except Exception as e:
                continue # 실패한 폴리곤은 건너뛰기
    except FileNotFoundError:
        print(f"오류: Polygon 파일 '{file_path}'를 찾을 수 없습니다. 경로를 확인해주세요.")
    except Exception as e:
        print(f"오류: Polygon 파일 '{file_path}' 로드 중 예상치 못한 오류 발생: {e}")
    return polygons

def calculate_location_score(
    row: Dict,
    hotplace_polys: Dict[str, Polygon],
    campus_polys: Dict[str, Polygon],
    new_hot_keywords: List[str]
) -> Dict:
    lat, lng = row.get("gps_latitude"), row.get("gps_longitude")
    address = str(row.get("address", ""))
    distance = pd.to_numeric(row.get("distance_from_subway", None), errors="coerce")

    # 1. 위경도 누락 체크
    if pd.isna(lat) or pd.isna(lng):
        return {
            "위치_점수": 0.0,
            "위치_산출근거": "gps 데이터 없음",
            "위치_실패사유": "gps 데이터 없음"
        }

    point = Point(lng, lat)

    # 2. 핫플레이스 Polygon (5점)
    for name, poly in hotplace_polys.items():
        if point.within(poly):
            return {
                "위치_점수": 5.0,
                "위치_산출근거": "핫플레이스",
                "위치_실패사유": "정상적으로 작동함"
            }

    # 3. 신규 핫플레이스 키워드 (4점) - 주소 기반 (Polygon 아님)
    for keyword in new_hot_keywords:
        if keyword in address:
            return {
                "위치_점수": 4.0,
                "위치_산출근거": "신규_핫플레이스",
                "위치_실패사유": "정상적으로 작동함"
            }

    # 4. 대학가 Polygon (4점)
    for name, poly in campus_polys.items():
        if point.within(poly):
            return {
                "위치_점수": 4.0,
                "위치_산출근거": "대학가",
                "위치_실패사유": "정상적으로 작동함"
            }

    # 5. 지하철 거리 기반 점수 (3점) - 이전 조건에 모두 미매칭 시
    if pd.isna(distance):
        return {
            "위치_점수": 0.0,
            "위치_산출근거": "지하철역 거리 데이터 없음",
            "위치_실패사유": "지하철역 거리 데이터 없음"
        }

    if distance <= 900: # 도보 15분 이내 (900m)
        return {
            "위치_점수": 3.0,
            "위치_산출근거": "거리_도보15분",
            "위치_실패사유": "정상적으로 작동함"
        }
    
    # 6. 모든 조건에서 미매칭 (0점 처리)
    # 현재 사용자 제공 로직에서는 900m 초과 시 바로 0점으로 처리되도록 되어 있습니다.
    # 2점 ('역에서 도보 25분 이내 (1000m 이상)') 또는 1점 ('역에서 버스 환승 필요') 기준을 추가하려면
    # 이 부분에 elif distance > 900: (2점 로직) 및 else: (1점 로직)을 추가해야 합니다.
    return {
        "위치_점수": 0.0,
        "위치_산출근거": "거리_900m초과" if distance > 900 else "조건_미매칭", # 900m 초과 시 거리_900m초과로 표기
        "위치_실패사유": "모든 매칭이 안되었음"
    }


def run_scoring_pipeline(raw_data: List[Dict], output_to_file: bool = True) -> List[Dict]:
    """
    크롤링된 원본 매장 데이터를 받아 LLM 스코어링 및 위치 점수 계산을 수행하고,
    최종 점수를 합산하여 처리된 데이터를 반환하는 파이프라인 함수.

    Args:
        raw_data (List[Dict]): 크롤링된 매장 데이터 목록 (딕셔너리 리스트).
                                각 딕셔너리는 'naver_id', 'name', 'gps_latitude', 'gps_longitude',
                                'category', 'review_category', 'theme_mood', 'theme_topic',
                                'theme_purpose', 'menu_list', 'review_info',
                                'distance_from_subway', 'on_tv', 'seoul_michelin',
                                'blog_review_count', 'parking_available' 등의 키를 포함해야 합니다.
        output_to_file (bool): 처리된 결과를 JSON 파일로 저장할지 여부. 기본값은 True.

    Returns:
        List[Dict]: LLM 스코어, 위치 점수, 최종 Total 점수 및 산출 근거가 추가된
                    매장 데이터 목록 (딕셔너리 리스트).
    """
    if not raw_data:
        print("경고: 처리할 원본 데이터가 비어있습니다. 파이프라인을 종료합니다.")
        return []

    # Few-shot 예시로 사용할 데이터를 포맷팅
    # 여기서는 입력받은 raw_data를 예시로 사용할 수 있습니다.
    TEST_EXAMPLES_FOR_PROMPT_STR = format_test_data_as_examples(raw_data)

    # Polygon 데이터 로드 (함수 호출 시마다 로드하는 것보다, 전역으로 로드된 것을 사용하는 것이 효율적)
    # 여기서는 이미 스크립트 시작 시 전역으로 로드된 hotplace_polys와 campus_polys를 사용합니다.
    print(f"\n--- 핫플레이스 Polygon 데이터 로드 중: {HOTSPOT_POLYGON_CSV} ---")
    hotplace_polys = load_polygons_from_df(HOTSPOT_POLYGON_CSV, "location", "polygon_str")
    print(f"로딩된 핫플레이스 Polygon 개수: {len(hotplace_polys)}")

    print(f"\n--- 대학가 Polygon 데이터 로드 중: {CAMPUS_POLYGON_CSV} ---")
    campus_polys = load_polygons_from_df(CAMPUS_POLYGON_CSV, "campus_name", "polygon_str")
    print(f"로딩된 대학가 Polygon 개수: {len(campus_polys)}")

    # 핫플레이스 인접(100m) 영역용 폴리곤 로딩
    print(f"\n--- 핫플레이스 인접(100m) Polygon 데이터 로드 중: {HOTSPOT_POLYGON_CSV} ---")
    donut_df = pd.read_csv(HOTSPOT_POLYGON_CSV)
    donut_polys = [wkt.loads(row["WKT_Polygon_100m_Donut"]) for _, row in donut_df.iterrows()]
    print(f"로딩된 핫플레이스 인접(100m) Polygon 개수: {len(donut_polys)}")
    
    new_hot_keywords = ["삼성역", "코엑스", "익선동", "샤로수길", "송리단길", "해방촌", "후암동", "서촌"]

    processed_store_data = []
    print(f"\n--- {len(raw_data)}개의 매장 정보 분류 시작 ---")

    for i, store_entry in enumerate(raw_data):
        # print(f"\n===== {i+1}/{len(raw_data)} 매장 분류: '{store_entry.get('name', '알 수 없음')}' =====")
        
        # 원본 데이터를 복사하여 LLM에 전달하고, 추론 결과를 여기에 추가합니다.
        # 이렇게 하면 원본 'store_entry' 딕셔너리에 직접 수정이 가해집니다.
        # 만약 원본 데이터를 건드리지 않고 새로운 딕셔너리를 만들고 싶다면 store_entry.copy()를 사용하세요.
        current_store_data = store_entry.copy() # 원본 데이터 보존을 위해 복사

        # 1. LLM 추론 결과 받기 (메뉴 관련 점수)
        llm_result = get_categorized_store_info(current_store_data, TEST_EXAMPLES_FOR_PROMPT_STR)

        # 2. 위치 점수 계산 (로드된 Polygon 데이터와 키워드를 전달)
        location_score_info = calculate_location_score(current_store_data, hotplace_polys, campus_polys, new_hot_keywords)

        # current_store_data에 LLM 추론 결과와 위치 점수 결과 추가
        # 메뉴 관련 필드 추가
        if llm_result:
            current_store_data["대분류"] = llm_result.get("대분류", "")
            current_store_data["중분류"] = llm_result.get("중분류", "")
            current_store_data["소분류"] = llm_result.get("소분류", "")
            current_store_data["메뉴_라벨"] = llm_result.get("메뉴_라벨", "")
            try:
                menu_score_from_llm = float(llm_result.get("메뉴_점수", 0))
            except (ValueError, TypeError):
                menu_score_from_llm = 0.0
            current_store_data["메뉴_점수"] = menu_score_from_llm
            current_store_data["메뉴_추론근거"] = llm_result.get("메뉴_추론근거", "")
        else:
            current_store_data["대분류"] = ""
            current_store_data["중분류"] = ""
            current_store_data["소분류"] = ""
            current_store_data["메뉴_라벨"] = ""
            current_store_data["메뉴_점수"] = 0.0
            current_store_data["메뉴_추론근거"] = "LLM 분류 중 오류 발생 또는 응답 파싱 실패"
        
        # 위치 관련 필드 추가
        current_store_data["위치_점수"] = location_score_info.get("위치_점수", 0.0)
        current_store_data["위치_산출근거"] = location_score_info.get("위치_산출근거", "")
        current_store_data["위치_실패사유"] = location_score_info.get("위치_실패사유", "")

        # 3. Total 점수 합산 (메뉴 점수, 위치 점수, 추가 조건 점수)
        base_total_score = (current_store_data["메뉴_점수"] + current_store_data.get("위치_점수", 0.0)) / 2
        additional_score = 0.0
        total_score_breakdown = []

        if current_store_data.get("on_tv") == True:
            additional_score += 0.3
            total_score_breakdown.append("방송 출연")

        if current_store_data.get("seoul_michelin") == True:
            additional_score += 0.5
            total_score_breakdown.append("서울 미쉐린 선정")

        blog_review_count = current_store_data.get("blog_review_count")
        if isinstance(blog_review_count, (int, float)) and blog_review_count >= 300:
            additional_score += 0.3
            total_score_breakdown.append(f"블로그 리뷰 300개 이상 ({int(blog_review_count)}개)")

        if current_store_data.get("parking_available") == True:
            additional_score += 0.2
            total_score_breakdown.append("주차 가능")

        # 핫플레이스 인접(100m) 영역 판별 (Total 점수 가산용)
        lat = current_store_data.get("gps_latitude")
        lng = current_store_data.get("gps_longitude")
        if lat and lng:
            try:
                point = Point(lng, lat)
                for poly in donut_polys:
                    if point.within(poly):
                        additional_score += 0.5
                        total_score_breakdown.append("핫플레이스 인접(100m) 포함")
                        detailed_additional_items.append("핫플레이스 인접(100m) 포함(0.5점)")
                        break
            except Exception:
                pass

        final_total_score = base_total_score + additional_score
        current_store_data["Total_점수"] = round(final_total_score, 1) # 소수점 첫째자리까지 반올림

        # Total_산출근거 구성
        reason_parts = [
            f"메뉴 점수({current_store_data['메뉴_점수']:.1f}점)",
            f"위치 점수({current_store_data['위치_점수']:.1f}점)"
        ]
        
        # 추가 점수 항목 세부 내용 구성 (방송출 연, 미쉐린 선정, 블로그 리뷰, 주차 가능, 핫스팟 인접 매장)
        detailed_additional_items = []
        if "방송 출연" in total_score_breakdown:
            detailed_additional_items.append("방송 출연(0.3점)")
        if "서울 미쉐린 선정" in total_score_breakdown:
            detailed_additional_items.append("서울 미쉐린 선정(0.5점)")
        if any("블로그 리뷰" in item for item in total_score_breakdown):
             for item in total_score_breakdown:
                 if "블로그 리뷰" in item:
                     detailed_additional_items.append(f"{item.replace(' (+0.3점)', '(0.3점)')}") # "블로그 리뷰 300개 이상 (N개)(0.3점)" 형태로
        if "주차 가능" in total_score_breakdown:
            detailed_additional_items.append("주차 가능(0.2점)")

        if "핫스팟 인접 매장" in current_store_data.get("위치_산출근거", ""):
            additional_score += 0.5
            total_score_breakdown.append("핫스팟 인접 매장")
            detailed_additional_items.append("핫스팟 인접 매장(0.5점)")

        if detailed_additional_items:
            current_store_data["Total_산출근거"] = (
                f"메뉴 점수({current_store_data['메뉴_점수']:.1f}점) + "
                f"위치 점수({current_store_data['위치_점수']:.1f}점) / 2 = {base_total_score:.1f}점; "
                f"추가 점수 항목: {', '.join(detailed_additional_items)}; "
                f"총 추가 점수: {additional_score:.1f}점"
            )
        else:
            current_store_data["Total_산출근거"] = (
                f"메뉴 점수({current_store_data['메뉴_점수']:.1f}점) + "
                f"위치 점수({current_store_data['위치_점수']:.1f}점) / 2 = {base_total_score:.1f}점; "
                f"추가 점수 항목 없음"
            )


        processed_store_data.append(current_store_data)
        print("분류 및 점수 계산 완료.")
        print("="*80)

    # 최종 결과 JSON 파일로 저장 (선택 사항)
    if output_to_file:
        today_date_str = datetime.date.today().strftime("%Y%m%d")
        output_filename = f"classified_store_data_{today_date_str}.json"
        output_filepath = os.path.join(JSON_DATA_DIR, output_filename)

        try:
            with open(output_filepath, 'w', encoding='utf-8') as f:
                json.dump(processed_store_data, f, indent=4, ensure_ascii=False)
            print(f"\n\n--- 모든 매장 정보 분류 완료! 결과가 '{output_filepath}'에 저장되었습니다. ---")
        except Exception as e:
            print(f"오류: 최종 JSON 파일 저장 중 오류 발생: {e}")

    return processed_store_data


# --- Main 실행 블록 (함수 호출 예시) ---
if __name__ == "__main__":
    # 이 부분은 실제 크롤링 모듈에서 데이터를 받아오는 것으로 대체될 수 있습니다.
    # 현재는 예시로 제공된 'store_crawling_20250523.json' 파일을 로드합니다.
    raw_data_filename = "store_crawling_20250523.json"
    
    # JSON_DATA_DIR을 사용하여 파일을 찾도록 변경
    full_raw_data_path = os.path.join(JSON_DATA_DIR, raw_data_filename) 
    
    # 로컬 테스트를 위해 현재 디렉토리에도 파일이 있는지 확인
    if not os.path.exists(full_raw_data_path):
        if os.path.exists(raw_data_filename):
            full_raw_data_path = raw_data_filename
        else:
            print(f"오류: 원본 매장 데이터 파일 '{raw_data_filename}'을 지정된 경로 '{JSON_DATA_DIR}'에서도, 현재 디렉토리에서도 찾을 수 없습니다. 경로를 확인해주세요.")
            sys.exit(1)

    input_raw_store_data = load_json_data(full_raw_data_path)

    if input_raw_store_data is None:
        print(f"오류: 원본 매장 데이터 파일 '{full_raw_data_path}'를 로드할 수 없습니다. 프로그램 종료.")
        sys.exit(1)

    # 파이프라인 실행 및 결과 받기
    final_classified_data = run_scoring_pipeline(input_raw_store_data, output_to_file=True)

    # print("\n--- 최종 분류 결과 (JSON 배열 - 첫 2개 항목 예시) ---")
    # if final_classified_data:
    #     print(json.dumps(final_classified_data[:2], indent=4, ensure_ascii=False))
    #     if len(final_classified_data) > 2:
    #         print("...")
    # else:
    #     print("처리된 데이터가 없습니다.")