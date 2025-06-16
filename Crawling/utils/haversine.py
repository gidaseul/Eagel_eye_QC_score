import math

# 두 위도-경도 값 사이의 거리를 계산하고 반환합니다.


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # 위도, 경도 -> 라디안으로 변환
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    diff_lat = lat2 - lat1
    diff_lon = lon2 - lon1

    # 하버사인 공식 적용
    a = math.sin(diff_lat/2)**2 + math.cos(lat1) * \
        math.cos(lat2) * math.sin(diff_lon/2)**2
    c = 2 * math.asin(math.sqrt(a))

    # 지구의 반지름 in meter
    r = 6371000

    # 계산 결과 in meter
    return round(c * r, 1)


# x1 = 126.9234511
# y1 = 37.5550418


# x2 = 126.924332
# y2 = 37.553438

# print(haversine(y1, x1, y2, x2))
