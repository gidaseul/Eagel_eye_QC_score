import pandas as pd
import os
import boto3
from io import BytesIO

def load_ids_from_master_data(storage_mode, config) -> set:
    """
    스토리지 모드에 따라 total/ 폴더에서 가장 최신 마스터 JSON 파일을 찾아
    naver_id 목록을 set으로 반환합니다.
    """
    print("마스터 데이터에서 naver_id 목록 로딩을 시작합니다.")
    master_file_to_read = None

    try:
        if storage_mode == 's3':
            s3_config = config['s3_config']
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(Bucket=s3_config['bucket_name'], Prefix=s3_config['total_results_prefix'])
            if 'Contents' not in response:
                raise FileNotFoundError

            all_master_files = [
                obj['Key'] for obj in response['Contents']
                if obj['Key'].split('/')[-1].startswith(s3_config['master_file_prefix']) and obj['Key'].endswith('.json')
            ]
            if not all_master_files:
                raise FileNotFoundError
            master_file_to_read = max(all_master_files)

            obj = s3_client.get_object(Bucket=s3_config['bucket_name'], Key=master_file_to_read)
            json_bytes = obj['Body'].read()
            df = pd.read_json(BytesIO(json_bytes))

        else:  # local mode
            local_config = config['local_config']
            total_dir = local_config['total_dir']
            if not os.path.exists(total_dir):
                raise FileNotFoundError

            all_master_files = [
                f for f in os.listdir(total_dir)
                if f.startswith(local_config['master_file_prefix']) and f.endswith('.json')
            ]
            if not all_master_files:
                raise FileNotFoundError
            master_file_to_read = max(all_master_files)

            df = pd.read_json(os.path.join(total_dir, master_file_to_read))

        print(f"마스터 파일 '{master_file_to_read}'에서 ID 목록을 추출합니다.")
        if 'naver_id' not in df.columns:
            raise ValueError("'naver_id' 컬럼 없음")

        id_set = set(pd.to_numeric(df['naver_id'], errors='coerce').dropna().astype(int))
        print(f"ID 목록 로딩 완료. 총 {len(id_set)}개의 고유 ID를 불러왔습니다.")
        return id_set

    except FileNotFoundError:
        print("정보: 통합 마스터 파일이 없어 빈 ID 목록으로 시작합니다.")
        return set()
    except Exception as e:
        print(f"오류: 마스터 데이터 로딩 실패. {e}")
        return set()