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
from tqdm import tqdm
# ----------------------------------------------------------------------

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


def generate_categorization_prompt(store_data: Dict, additional_examples_str: str, category_map_str: str, score_map_str: str) -> str:
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
{category_map_str}
</CATEGORY_MAPPING_DATA>

### 라벨별 점수 매핑 정보:
<SCORE_MAPPING_DATA>
{score_map_str}
</SCORE_MAPPING_DATA>

{user_input_text}

---
위의 지시사항, 예시, 매핑 데이터 그리고 현재 매장 정보를 바탕으로 가장 적절한 대분류, 중분류, 소분류, 라벨, 점수를 추론하고, 그 추론 근거를 포함하여 정확히 다음 JSON 스키마에 맞춰 응답하세요.
(응답할 JSON 스키마: {json.dumps(StoreCategoryResponse.model_json_schema(), ensure_ascii=False, indent=2)})
응답:
"""
    return full_prompt


def get_categorized_store_info(store_data: Dict, additional_examples_str: str, category_map_str: str, score_map_str: str) -> Optional[Dict]:
    """
    주어진 단일 매장 정보를 Gemini 모델에 보내어 카테고리 분류 결과를 받습니다.
    additional_examples_str 인자를 추가하여 동적으로 추가 예시를 전달합니다.
    """
    prompt = generate_categorization_prompt(store_data, additional_examples_str, category_map_str, score_map_str)

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


def run_scoring_pipeline(input_data: List[Dict], data_dir: str) -> List[Dict]:
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
    if not input_data:
        print("경고: 처리할 원본 데이터가 비어있습니다. 파이프라인을 종료합니다.")
        return []

    # --- [수정] 함수 내부에서 필요한 데이터를 인자로 받은 data_dir을 사용해 로드 ---
    print("점수 산정용 데이터 로딩 시작...")
    category_mapping = load_json_data(os.path.join(data_dir, 'category_mapping.json'))
    score_mapping = load_json_data(os.path.join(data_dir, 'score_mapping_54321.json'))
    hotspot_polys = load_polygons_from_df(os.path.join(data_dir, "seoul_hotspots_polygons.csv"), "location", "polygon_str")
    campus_polys = load_polygons_from_df(os.path.join(data_dir, "campus_polygons.csv"), "campus_name", "polygon_str")
    
    donut_polys = []
    try:
        donut_df = pd.read_csv(os.path.join(data_dir, "seoul_hotspots_polygons.csv"))
        donut_polys = [wkt.loads(row["WKT_Polygon_100m_Donut"]) for _, row in donut_df.iterrows()]
    except Exception as e:
        print(f"경고: 핫플레이스 인접 영역(Donut) Polygon 로딩 실패 - {e}", file=sys.stderr)

    if not all([category_mapping, score_mapping, hotspot_polys, campus_polys]):
        print("오류: 점수 산정에 필요한 데이터 파일 로딩에 실패했습니다. 파이프라인을 중단합니다.", file=sys.stderr)
        return input_data

    category_map_str = json.dumps(category_mapping, ensure_ascii=False)
    score_map_str = json.dumps(score_mapping, ensure_ascii=False)
    new_hot_keywords = ["삼성역", "코엑스", "익선동", "샤로수길", "송리단길", "해방촌", "후암동", "서촌"]

    # Few-shot 예시를 위한 데이터 포맷팅
    test_examples_for_prompt_str = format_test_data_as_examples(input_data)

    processed_data = []
    print(f"\n{len(input_data)}개의 매장 정보에 대한 점수 산정을 시작합니다.")

    for store_entry in tqdm(input_data, desc="Scoring Progress"):
        current_store = store_entry.copy()

        # 1. LLM 추론 결과 받기 (메뉴 관련 점수)
        llm_result = get_categorized_store_info(current_store, test_examples_for_prompt_str, category_map_str, score_map_str)

        # 2. 위치 점수 계산 (로드된 Polygon 데이터와 키워드를 전달)
        location_result = calculate_location_score(current_store, hotspot_polys, campus_polys, new_hot_keywords)

        # current_store에 LLM 추론 결과와 위치 점수 결과 추가
        # 메뉴 관련 필드 추가
        if llm_result:
            current_store["대분류"] = llm_result.get("대분류", "")
            current_store["중분류"] = llm_result.get("중분류", "")
            current_store["소분류"] = llm_result.get("소분류", "")
            current_store["메뉴_라벨"] = llm_result.get("메뉴_라벨", "")
            try:
                menu_score_from_llm = float(llm_result.get("메뉴_점수", 0))
            except (ValueError, TypeError):
                menu_score_from_llm = 0.0
            current_store["메뉴_점수"] = menu_score_from_llm
            current_store["메뉴_추론근거"] = llm_result.get("메뉴_추론근거", "")
        else:
            current_store["대분류"] = ""
            current_store["중분류"] = ""
            current_store["소분류"] = ""
            current_store["메뉴_라벨"] = ""
            current_store["메뉴_점수"] = 0.0
            current_store["메뉴_추론근거"] = "LLM 분류 중 오류 발생 또는 응답 파싱 실패"
        
        # 위치 관련 필드 추가
        current_store["위치_점수"] = location_result.get("위치_점수", 0.0)
        current_store["위치_산출근거"] = location_result.get("위치_산출근거", "")
        current_store["위치_실패사유"] = location_result.get("위치_실패사유", "")

        # 3. Total 점수 합산 (메뉴 점수, 위치 점수, 추가 조건 점수)
        base_total_score = (current_store["메뉴_점수"] + current_store.get("위치_점수", 0.0)) / 2
        additional_score = 0.0
        total_score_breakdown = []

        if current_store.get("on_tv") == True:
            additional_score += 0.3
            total_score_breakdown.append("방송 출연")

        if current_store.get("seoul_michelin") == True:
            additional_score += 0.5
            total_score_breakdown.append("서울 미쉐린 선정")

        blog_review_count = current_store.get("blog_review_count")
        if isinstance(blog_review_count, (int, float)) and blog_review_count >= 300:
            additional_score += 0.3
            total_score_breakdown.append(f"블로그 리뷰 300개 이상 ({int(blog_review_count)}개)")

        if current_store.get("parking_available") == True:
            additional_score += 0.2
            total_score_breakdown.append("주차 가능")

        # 핫플레이스 인접(100m) 영역 판별 (Total 점수 가산용)
        lat = current_store.get("gps_latitude")
        lng = current_store.get("gps_longitude")
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
        current_store["Total_점수"] = round(final_total_score, 1) # 소수점 첫째자리까지 반올림

        # Total_산출근거 구성
        reason_parts = [
            f"메뉴 점수({current_store['메뉴_점수']:.1f}점)",
            f"위치 점수({current_store['위치_점수']:.1f}점)"
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

        if "핫스팟 인접 매장" in current_store.get("위치_산출근거", ""):
            additional_score += 0.5
            total_score_breakdown.append("핫스팟 인접 매장")
            detailed_additional_items.append("핫스팟 인접 매장(0.5점)")

        if detailed_additional_items:
            current_store["Total_산출근거"] = (
                f"메뉴 점수({current_store['메뉴_점수']:.1f}점) + "
                f"위치 점수({current_store['위치_점수']:.1f}점) / 2 = {base_total_score:.1f}점; "
                f"추가 점수 항목: {', '.join(detailed_additional_items)}; "
                f"총 추가 점수: {additional_score:.1f}점"
            )
        else:
            current_store["Total_산출근거"] = (
                f"메뉴 점수({current_store['메뉴_점수']:.1f}점) + "
                f"위치 점수({current_store['위치_점수']:.1f}점) / 2 = {base_total_score:.1f}점; "
                f"추가 점수 항목 없음"
            )


        processed_data.append(current_store)
        print("분류 및 점수 계산 완료.")
        print("="*80)

    # ▼▼▼ [수정] 파일 저장 로직 제거, 처리된 데이터를 return ▼▼▼
    print("모든 매장의 점수 산정이 완료되었습니다.")
    return processed_data