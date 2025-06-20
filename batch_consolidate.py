import pandas as pd
import yaml
import os
import glob
from datetime import datetime
import boto3
from io import BytesIO
import traceback
from typing import List, Dict, Any
from Crawling.utils.master_loader import load_ids_from_master_data

def run_consolidation_job():
    """
    모든 개별 결과 파일을 읽어, 중복을 제거한 최신 마스터 파일 1개만 남기고
    이전 마스터 파일은 삭제하는 데이터 통합 작업을 수행합니다.
    """
    try:
        config = yaml.safe_load(open("config.yaml", 'r', encoding='utf-8'))
        storage_mode = config.get('storage_mode', 'local')
        print(f"데이터 통합 배치 작업을 시작합니다. (스토리지 모드: {storage_mode.upper()})")

        # 1. 모드에 따라 모든 개별 결과 파일 목록 가져오기 및 읽기
        df_list = []
        if storage_mode == 's3':
            s3_config = config['s3_config']
            s3_client = boto3.client('s3')
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=s3_config['bucket_name'], Prefix=s3_config['output_results_prefix'])
            all_files = [obj['Key'] for page in pages if "Contents" in page for obj in page['Contents'] if obj['Key'].endswith('.json')]
            for file_key in all_files:
                response = s3_client.get_object(Bucket=s3_config['bucket_name'], Key=file_key)
                json_content = response['Body'].read().decode('utf-8')
                if json_content:
                    df_list.append(pd.read_json(BytesIO(json_content.encode('utf-8'))))
        else:  # local mode
            local_config = config['local_config']
            search_path = os.path.join(local_config['output_dir'], "**", "*.json")
            all_files = glob.glob(search_path, recursive=True)
            for file_path in all_files:
                df_list.append(pd.read_json(file_path))

        if not df_list:
            print("통합할 결과 파일이 없습니다.")
            return

        # 2. 통합 및 중복 제거 (naver_id 기준, 가장 마지막에 수집된 데이터 유지)
        master_df = pd.concat(df_list, ignore_index=True)
        master_df.drop_duplicates(subset=['naver_id'], keep='last', inplace=True)
        print(f"총 {len(all_files)}개 파일 통합, 중복 제거 후 {len(master_df)}건 데이터 생성.")

        # 3. 새 통합 파일 생성
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # 4. 이전 통합 파일 삭제 및 새 파일 저장
        if storage_mode == 's3':
            s3_config = config['s3_config']
            total_prefix = s3_config['total_results_prefix']
            master_prefix = s3_config['master_file_prefix']
            new_master_key = f"{total_prefix}{master_prefix}_{timestamp}.json"

            response = s3_client.list_objects_v2(Bucket=s3_config['bucket_name'], Prefix=total_prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].split('/')[-1].startswith(master_prefix):
                        s3_client.delete_object(Bucket=s3_config['bucket_name'], Key=obj['Key'])

            json_bytes = master_df.to_json(orient='records', force_ascii=False).encode('utf-8')
            json_buffer = BytesIO(json_bytes)
            s3_client.put_object(Bucket=s3_config['bucket_name'], Key=new_master_key, Body=json_buffer.getvalue())
            print(f"새 통합 파일 생성: s3://{s3_config['bucket_name']}/{new_master_key}")
        else:  # local mode
            local_config = config['local_config']
            total_dir = local_config['total_dir']
            master_prefix = local_config['master_file_prefix']
            os.makedirs(total_dir, exist_ok=True)
            new_master_filepath = os.path.join(total_dir, f"{master_prefix}_{timestamp}.json")

            for old_file in os.listdir(total_dir):
                if old_file.startswith(master_prefix):
                    os.remove(os.path.join(total_dir, old_file))

            master_df.to_json(new_master_filepath, orient='records', force_ascii=False, indent=4)
            print(f"새 통합 파일 생성: {new_master_filepath}")

        print("데이터 통합 배치 작업을 성공적으로 마쳤습니다.")
    except Exception as e:
        print(f"배치 작업 중 오류 발생: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    run_consolidation_job()