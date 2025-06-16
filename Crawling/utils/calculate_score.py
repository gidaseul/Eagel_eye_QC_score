import pandas as pd


def calculate_score(series):
    score = 0

    score += 15 if series['new_store'] else 0

    # 신규 매장일 경우에만 DataLab 확인
    if series['new_store']:
        score += 7 if series['gender-balance'] else 0
        score += 7 if series['age-2030'] else 0
    else:
        score += 4 if series['gender-balance'] else 0
        score += 4 if series['age-2030'] else 0
        pass

    score += 3 if series['parking_available'] else 0

    # 인스타그램 계정 존재 여부
    if pd.isna(series['instagram_link']) or series['instagram_link'] == "" or "False":
        score -= 10

    # 역으로부터 거리
    if series['distance_from_subway'] == None:
        pass
    elif int(series['distance_from_subway']) <= 400:
        score += 3
    elif int(series['distance_from_subway']) <= 700:
        score += 2

    # 최근 매장 운영 상태 -> 기존 값에서 상황은 없을 거라고 생각 되어서 변경함.
    if series["running_well"] == 0 and series["new_store"]:
        score += 6
    elif series["running_well"] == 0:
        score += 4
    elif series['running_well'] == 1:
        score += 2
    elif series['running_well'] == 2:
        score += 0
    elif series['running_well'] == 3:
        score -= 2

    # 감점 요인
    if series["blog_review_count"] >= 900 or series["visitor_review_count"] >= 1000:
        score -= 10
    if series['instagram_follower'] >= 3000:
        score -= 10

    if series['on_tv']:
        score -= 5
    if series['seoul_michelin']:
        score -= 10

    if score < 0:
        return 0

    return score


# if __name__ == "__main__":
#     df = pd.DataFrame([{"new_store": True, "gender-balance": True, "age-2030": True, "hot_spot": True,
#                       "parking_available": True, "no_kids": True, "instagram_link": " ",
#                        "distance_from_subway": 301, "blog_review_count": 500, "visitor_review_count": 300}])

#     df['score'] = df.apply(calculate_score, axis=1)

#     print(df)
