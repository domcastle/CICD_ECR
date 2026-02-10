#!/usr/bin/env python3
import os
import json
import time
import tempfile
import subprocess
import redis
import boto3
from botocore.exceptions import ClientError

# --- 환경변수 로드 ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "video_processing_jobs")

# AWS S3 설정
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
# [수정] 버킷 이름 고정 (환경변수 없으면 이 값 사용)
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "team1videostorage-justic")

# 스크립트 경로
FFMPEG_SCRIPT = os.getenv("FFMPEG_SCRIPT", "/opt/ai/scripts/run_ffmpeg_shorts.sh")
CAPTION_SCRIPT = os.getenv("CAPTION_SCRIPT", "/opt/ai/worker/generate_caption.py")

# --- Redis 연결 ---
print(f"🔌 Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

# --- AWS 클라이언트 연결 ---
print(f"☁️  Initializing AWS Clients (Region: {AWS_REGION})...")
s3_client = boto3.client('s3', region_name=AWS_REGION)
# [추가] EC2 IP를 찾기 위한 클라이언트 추가
ec2_client = boto3.client('ec2', region_name=AWS_REGION)

# ---------------------------------------------------------
# [추가] EC2 자동 탐색 함수
# ---------------------------------------------------------
def get_ollama_server_ip():
    target_name = "ai-worker-cpu"
    print(f"🔍 Searching for EC2 instance named '{target_name}'...")
    try:
        response = ec2_client.describe_instances(
            Filters=[
                {'Name': 'tag:Name', 'Values': [target_name]},
                {'Name': 'instance-state-name', 'Values': ['running']}
            ]
        )
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                public_ip = instance.get('PublicIpAddress')
                if public_ip:
                    print(f"✅ Found Server: {public_ip}")
                    return f"http://{public_ip}:11434"
        return None
    except Exception as e:
        print(f"❌ AWS API Error: {e}")
        return None

# [추가] 시작할 때 IP 찾아서 저장 (못 찾으면 로컬호스트)
CURRENT_OLLAMA_HOST = get_ollama_server_ip()
if not CURRENT_OLLAMA_HOST:
    print("⚠️  Ollama server not found. Using localhost.")
    CURRENT_OLLAMA_HOST = "http://localhost:11434"


def download_object(key, dst):
    """S3에서 파일을 다운로드합니다."""
    try:
        print(f"⬇️  Downloading s3://{AWS_S3_BUCKET}/{key} -> {dst}")
        s3_client.download_file(AWS_S3_BUCKET, key, dst)
    except ClientError as e:
        print(f"❌ Download failed: {e}")
        raise

def upload_object(key, src):
    """S3로 파일을 업로드합니다."""
    try:
        print(f"⬆️  Uploading {src} -> s3://{AWS_S3_BUCKET}/{key}")
        s3_client.upload_file(
            src, 
            AWS_S3_BUCKET, 
            key, 
            ExtraArgs={'ContentType': 'video/mp4'}
        )
    except ClientError as e:
        print(f"❌ Upload failed: {e}")
        raise

def process_job(job: dict):
    input_key = job["input_key"]
    output_key = job["output_key"]
    variant = job.get("variant", "v1")

    # 임시 파일 생성
    tmp_input = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
    tmp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name

    try:
        # 1. S3 다운로드
        download_object(input_key, tmp_input)

        # 2. 캡션 생성 (subprocess)
        print(f"🧠 Generating caption via Ollama ({CURRENT_OLLAMA_HOST})...")
        
        # [수정] 환경변수에 찾은 IP 주입
        env = os.environ.copy()
        env["CAPTION_VARIANT"] = variant
        env["OLLAMA_HOST"] = CURRENT_OLLAMA_HOST  # <--- 여기서 IP 전달
        
        caption = ""
        try:
            caption = subprocess.check_output(
                ["python3", CAPTION_SCRIPT, tmp_input],
                text=True,
                timeout=600,
                env=env, # [수정] 조작된 환경변수 전달
            ).strip()
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Caption generation failed: {e}")
        except subprocess.TimeoutExpired:
            print("⚠️ Caption generation timed out.")
        
        if not caption:
            caption = "편집된 영상입니다"

        print(f"📝 Caption: {caption}")

        # 3. FFmpeg 실행 (subprocess)
        print("🎬 Processing video with FFmpeg...")
        subprocess.run(
            [
                FFMPEG_SCRIPT,
                tmp_input,
                tmp_output,
                "", # TTS Wav (없음)
                "", # Subtitle (없음)
                caption,
            ],
            check=True,
        )

        # 4. S3 업로드
        upload_object(output_key, tmp_output)
        print("✅ Job completed successfully.")

    except Exception as e:
        print(f"❌ Error processing job: {e}")
    finally:
        # 임시 파일 정리
        for f in (tmp_input, tmp_output):
            if os.path.exists(f):
                os.remove(f)

def main():
    print(f"🚀 AI Worker started (Target Ollama: {CURRENT_OLLAMA_HOST})")
    
    while True:
        try:
            # Redis 큐 대기
            result = redis_client.brpop(REDIS_QUEUE, timeout=5)
            if result:
                _, raw = result
                # 데이터 파싱 (bytes 대응)
                if isinstance(raw, bytes):
                    raw = raw.decode('utf-8')
                    
                job = json.loads(raw)
                print(f"📥 Received job: {job}")
                process_job(job)
                
        except redis.exceptions.ConnectionError:
            print("⚠️ Redis connection lost. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()