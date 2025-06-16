import csv
import os
import pandas as pd

# 파일 이름을 기준으로 블루리본서베이 매장 정보를 load해 반환합니다.


def load_bluer(file_name):
    bluer_data = pd.DataFrame(
        columns=["name", "ribbonType", "latitude", "longitude", "address", "phone"])

    # 스크립트 파일이 위치한 디렉토리의 상위 폴더를 기준으로 상대 경로 구하기
    directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_file_path = os.path.join(directory, 'data/blueribbon', file_name)

    # CSV 파일 읽기
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)

        for row in reader:
            name = row[0]
            ribbon_type = row[1]
            latitude = row[2]
            longitude = row[3]
            address = row[4]
            phone = row[5]

            new_data = pd.DataFrame([{
                "name": name,
                "ribbon_type": ribbon_type,
                "latitude": latitude,
                "longitude": longitude,
                "address": address,
                "phone": phone
            }])

            bluer_data = pd.concat([bluer_data, new_data], ignore_index=True)

    return bluer_data
