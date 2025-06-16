import boto3
import os

def upload_to_s3(local_dir, bucket_name, s3_prefix=""):
    s3 = boto3.client("s3")
    for filename in os.listdir(local_dir):
        if filename.endswith(".csv") or filename.endswith(".json"):
            local_path = os.path.join(local_dir, filename)
            s3_key = os.path.join(s3_prefix, filename)

            try:
                s3.upload_file(local_path, bucket_name, s3_key)
                print(f"☁️ S3 업로드 성공: {s3_key}")
            except Exception as e:
                print(f"❌ S3 업로드 실패: {s3_key} - {e}")
