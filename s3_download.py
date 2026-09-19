import os
import sys
import csv
import argparse
import boto3

def get_record_count(file_path):
    """
    따옴표로 묶인 값 안의 개행 문자를 처리하고, 헤더를 제외한 순수 레코드 수를 계산
    """
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        try:
            next(reader)  # 헤더 제외
        except StopIteration:
            return 0
        return sum(1 for _ in reader)

def main():
    parser = argparse.ArgumentParser(description="Download and analyze S3 files.")
    parser.add_argument(
        "--bucket", 
        type=str, 
        default=os.environ.get("S3_BUCKET_NAME"),
        help="S3 Bucket Name (기본값: S3_BUCKET_NAME 환경변수)"
    )
    args = parser.parse_args()

    bucket_name = args.bucket
    if not bucket_name:
        print("에러: 버킷 이름을 인자(--bucket) 또는 S3_BUCKET_NAME 환경변수로 지정해야 합니다.")
        sys.exit(1)

    prefix = "bronze/"
    target_key = "bronze/netflix_titles.csv"
    local_dir = "data"
    local_file_path = os.path.join(local_dir, "netflix_titles.csv")

    s3 = boto3.client("s3")

    # [1] bronze/ 아래 객체 목록과 각 객체의 크기 출력
    print(f"[1] list    s3://{bucket_name}/{prefix}")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    contents = response.get("Contents", [])
    for obj in contents:
        key = obj["Key"]
        size = obj["Size"]
        # 크기를 포맷팅하여 출력
        print(f"    {key:<35} {size:>15,} bytes")

    # [2] netflix_titles.csv를 project/data/ 아래로 다운로드
    os.makedirs(local_dir, exist_ok=True)
    s3.download_file(bucket_name, target_key, local_file_path)
    downloaded_size = os.path.getsize(local_file_path)
    print(f"[2] download s3://{bucket_name}/{target_key} -> {local_file_path}")
    print(f"    downloaded ({downloaded_size:,} bytes)")

    # [3] 내려받은 파일의 CSV 레코드 행 수 출력 (헤더 제외)
    row_count = get_record_count(local_file_path)
    print(f"[3] rows    {row_count}")

if __name__ == "__main__":
    main()