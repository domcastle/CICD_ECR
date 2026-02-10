import os
import boto3
from botocore.exceptions import ClientError

# worker.py와 동일한 환경변수 사용
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "videos")

# S3 클라이언트 초기화
# ROSA의 IRSA(IAM Role)를 쓰거나, 환경변수(ACCESS_KEY)가 있으면 자동 인식됨
s3_client = boto3.client('s3', region_name=AWS_REGION)

def ensure_bucket():
    """S3는 보통 미리 생성되므로 확인 로직만 두거나 생략 가능"""
    pass

# ======================
# 업로드 로직
# ======================
def upload_video(user_id: str, task_id: str, file_path: str, processed: bool = False):
    """로컬 파일을 S3로 업로드"""
    filename = f"{task_id}_processed.mp4" if processed else f"{task_id}.mp4"
    key = f"{user_id}/{filename}"
    
    print(f"⬆️ Uploading to S3: {key}")
    try:
        s3_client.upload_file(
            file_path, 
            AWS_S3_BUCKET, 
            key, 
            ExtraArgs={'ContentType': 'video/mp4'}
        )
    except ClientError as e:
        print(f"❌ S3 Upload Error: {e}")
        raise

def upload_thumbnail(user_id: str, task_id: str, thumb_path: str):
    """썸네일 이미지를 S3로 업로드"""
    key = f"{user_id}/{task_id}.jpg"
    
    print(f"⬆️ Uploading Thumbnail to S3: {key}")
    try:
        s3_client.upload_file(
            thumb_path, 
            AWS_S3_BUCKET, 
            key, 
            ExtraArgs={'ContentType': 'image/jpeg'}
        )
    except ClientError as e:
        print(f"❌ S3 Thumbnail Upload Error: {e}")
        raise

# ======================
# 스트리밍/다운로드 로직
# ======================
def get_video_stream(user_id: str, task_id: str, processed: bool = False):
    """S3 객체 Body 반환 (FastAPI StreamingResponse용)"""
    filename = f"{task_id}_processed.mp4" if processed else f"{task_id}.mp4"
    key = f"{user_id}/{filename}"
    
    try:
        obj = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=key)
        return obj['Body']
    except ClientError as e:
        print(f"❌ S3 Stream Error: {e}")
        raise

def get_thumbnail_stream(user_id: str, task_id: str):
    key = f"{user_id}/{task_id}.jpg"
    try:
        obj = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=key)
        return obj['Body']
    except ClientError as e:
        # 썸네일이 없을 때 처리를 위해 에러 전파
        raise

# ======================
# 리스트 로직
# ======================
def list_user_videos(user_id: str):
    """해당 유저의 S3 영상 목록 조회"""
    prefix = f"{user_id}/"
    try:
        response = s3_client.list_objects_v2(Bucket=AWS_S3_BUCKET, Prefix=prefix)
        if 'Contents' not in response:
            return []
        
        results = []
        for obj in response['Contents']:
            # key 예시: user123/taskABC.mp4
            key = obj['Key']
            filename = key.split("/")[-1]
            
            if filename.endswith(".mp4"):
                # 확장자 제거한 이름 반환 (기존 로직 유지)
                results.append(filename.replace(".mp4", ""))
                
        return sorted(results, reverse=True)
    except ClientError as e:
        print(f"❌ S3 List Error: {e}")
        return []