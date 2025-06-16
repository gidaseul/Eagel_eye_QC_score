# from datetime import datetime, timedelta

# # 크롤링 과정에서 최근 영업상태를 나타내는 feature의 값을 결정지을 때 사용됩니다


# def is_within_one_month(date_text):
#     # 방문일이 현재 날짜 기준으로 1개월 이내인지 확인.
#     date_text_formatted = ' '.join(date_text.split(' ')[:3])
#     date = datetime.strptime(date_text_formatted, "%Y년 %m월 %d일")
#     current_date = datetime.now()
#     return current_date - timedelta(days=30) <= date <= current_date


# def is_within_two_weeks(date_text):
#     # 요일 정보를 제외하고 날짜만 추출
#     date_text_formatted = ' '.join(date_text.split(' ')[:3])
#     date = datetime.strptime(date_text_formatted, "%Y년 %m월 %d일")

#     # 현재 날짜
#     current_date = datetime.now()

#     # 날짜가 두 주 이내인지 확인
#     return current_date - timedelta(days=14) <= date <= current_date

import logging
import re
from datetime import datetime, date, timedelta

def parse_date(date_text):
    """
    네이버 방문일 날짜를 datetime 객체로 변환
    - '24.12.22.' → 2024-12-22
    - '2.22.' → 현재 연도의 2월 22일
    - '2025년 1월 24일 금요일' → 2025-01-24
    - '방문일 없음' 또는 잘못된 데이터 → None 반환
    """
    try:
        # datetime, date 객체면 바로 반환
        if isinstance(date_text, (datetime, date)):
            return date_text
        date_text = date_text.strip()
        # (f"🔍 방문일 원본 데이터: {date_text}")

        # 특정 패턴이 포함된 경우 변환 불가
        if not date_text or "방문일" in date_text or "--" in date_text:
            logging.warning(f"⚠️ 유효하지 않은 날짜 데이터: {date_text}")
            return None

        # 1️⃣ 'YYYY년 MM월 DD일 요일' 형식 처리
        match_korean = re.search(r'(\d{4})년 (\d{1,2})월 (\d{1,2})일', date_text)
        if match_korean:
            year, month, day = map(int, match_korean.groups())
            return datetime(year, month, day)

        # 2️⃣ 'YY.MM.DD.' 또는 'YYYY.MM.DD.' 형식 처리
        match_full = re.search(r'(\d{2,4})\.(\d{1,2})\.(\d{1,2})\.', date_text)
        if match_full:
            year, month, day = map(int, match_full.groups())
            # 2자리 연도 → 2000년대 기준 변환
            if year < 100:
                year += 2000  # 24 → 2024 변환
            return datetime(year, month, day)

        # 3️⃣ 'M.DD.' 형식 처리 (연도 없음 → 현재 연도로 설정)
        match_short = re.search(r'(\d{1,2})\.(\d{1,2})\.', date_text)
        if match_short:
            month, day = map(int, match_short.groups())
            year = datetime.now().year  # 현재 연도 사용
            return datetime(year, month, day)

        logging.warning(f"⚠️ 날짜 변환 실패: {date_text}")
        return None

    except Exception as e:
        logging.warning(f"❌ 날짜 파싱 중 오류 발생: {e}")
        return None

def is_within_three_months(date_val):
    """
    방문일이 현재 날짜 기준으로 3개월 이내인지 확인.
    date_val: datetime, date, 또는 문자열
    """
    if isinstance(date_val, (datetime, date)):
        date_obj = date_val
    else:
        date_obj = parse_date(date_val)
    if date_obj is None:
        return False
    now = datetime.now()
    # 타입 맞추기
    if isinstance(date_obj, datetime):
        current_date = now
    else:
        current_date = now.date()
    three_month_ago = current_date - timedelta(days=90)
    return three_month_ago <= date_obj <= current_date

def is_within_one_month(date_val):
    """
    방문일이 현재 날짜 기준으로 1개월 이내인지 확인.
    date_val: datetime, date, 또는 문자열
    """
    if isinstance(date_val, (datetime, date)):
        date_obj = date_val
    else:
        date_obj = parse_date(date_val)
    if date_obj is None:
        return False
    now = datetime.now()
    if isinstance(date_obj, datetime):
        current_date = now
    else:
        current_date = now.date()
    one_month_ago = current_date - timedelta(days=30)
    return one_month_ago <= date_obj <= current_date

def is_within_two_weeks(date_val):
    """
    방문일이 현재 날짜 기준으로 2주 이내인지 확인.
    date_val: datetime, date, 또는 문자열
    """
    if isinstance(date_val, (datetime, date)):
        date_obj = date_val
    else:
        date_obj = parse_date(date_val)
    if date_obj is None:
        return False
    now = datetime.now()
    if isinstance(date_obj, datetime):
        current_date = now
    else:
        current_date = now.date()
    two_weeks_ago = current_date - timedelta(days=14)
    return two_weeks_ago <= date_obj <= current_date

