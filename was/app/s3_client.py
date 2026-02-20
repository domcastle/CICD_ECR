import os
import boto3
from botocore.exceptions import ClientError

# 환경변수 설정 (기존 배포 환경과 동일)
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "team1videostorage-justic")

# S3 클라이언트 초기화
s3_client = boto3.client('s3', region_name=AWS_REGION)

def ensure_bucket():
    """S3 버킷 존재 확인 (필요 시 로직 추가)"""
    pass

# ==============================
# 1. 업로드 로직 (v1, v2 네이밍 반영)
# ==============================
def upload_video(user_id: str, task_id: str, file_path: str, variant: str = None):
    """
    로컬 파일을 S3로 업로드합니다.
    - variant가 None이면: task_id.mp4 (원본)
    - variant가 v1이면: task_id_v1.mp4 (자막버전1)
    """
    filename = f"{task_id}_{variant}.mp4" if variant else f"{task_id}.mp4"
    key = f"{user_id}/{filename}"
    
    print(f"⬆️ S3 업로드 중: {key}")
    try:
        s3_client.upload_file(
            file_path, 
            AWS_S3_BUCKET, 
            key, 
            ExtraArgs={'ContentType': 'video/mp4'}
        )
    except ClientError as e:
        print(f"❌ S3 업로드 에러: {e}")
        raise

def upload_thumbnail(user_id: str, task_id: str, thumb_path: str):
    """썸네일 이미지를 S3로 업로드합니다."""
    key = f"{user_id}/{task_id}.jpg"
    
    print(f"⬆️ S3 썸네일 업로드 중: {key}")
    try:
        s3_client.upload_file(
            thumb_path, 
            AWS_S3_BUCKET, 
            key, 
            ExtraArgs={'ContentType': 'image/jpeg'}
        )
    except ClientError as e:
        print(f"❌ S3 썸네일 업로드 에러: {e}")
        raise

# ==============================
# 2. 스트리밍 로직 (variant 대응)
# ==============================
def get_video_stream(user_id: str, task_id: str, variant: str = None):
    """S3 객체의 Body(스트림)를 반환합니다."""
    filename = f"{task_id}_{variant}.mp4" if variant else f"{task_id}.mp4"
    key = f"{user_id}/{filename}"
    
    try:
        obj = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=key)
        return obj['Body']
    except ClientError as e:
        print(f"❌ S3 스트림 에러: {e} (Key: {key})")
        raise

def get_thumbnail_stream(user_id: str, task_id: str):
    """S3 썸네일 객체의 Body를 반환합니다."""
    key = f"{user_id}/{task_id}.jpg"
    try:
        obj = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=key)
        return obj['Body']
    except ClientError as e:
        print(f"❌ S3 썸네일 스트림 에러: {e}")
        raise

# ==============================
# 3. 리스트 로직 (기존 방식 유지)
# ==============================
def list_user_videos(user_id: str):
    """
    해당 유저의 모든 mp4 파일명을 반환합니다.
    중복 제거 없이 S3에 있는 모든 버전(원본, v1, v2)을 각각 리스트에 담습니다.
    """
    prefix = f"{user_id}/"
    try:
        response = s3_client.list_objects_v2(Bucket=AWS_S3_BUCKET, Prefix=prefix)
        if 'Contents' not in response:
            return []
        
        results = []
        for obj in response['Contents']:
            key = obj['Key']
            filename = key.split("/")[-1]
            
            # mp4 파일만 찾아서 확장자를 떼고 리스트에 추가
            if filename.endswith(".mp4"):
                # 결과 예시: ["task123", "task123_v1", "task123_v2"]
                results.append(filename.replace(".mp4", ""))
                
        # 최신순 정렬 (파일명 기준 내림차순)
        return sorted(results, reverse=True)
    except ClientError as e:
        print(f"❌ S3 목록 조회 에러: {e}")
        return []