# app/video.py
import os
import json
import httpx
import subprocess
import tempfile
import redis
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from security import verify_jwt
# [수정] MinIO 대신 S3 Client 사용
from s3_client import (
    upload_video,
    upload_thumbnail,
    get_video_stream,
    get_thumbnail_stream,
    list_user_videos,
)

from ai import (
    insert_final_video,
    mark_youtube_uploaded,
    insert_operation_log,
)
from google_auth import get_youtube_service
from googleapiclient.http import MediaFileUpload

router = APIRouter(tags=["video"])

# ==============================
# KIE (Text -> Video)
# ==============================
KIE_API_URL = "https://api.kie.ai/api/v1/veo/generate"
KIE_API_KEY = os.getenv("KIE_API_KEY")

# ==============================
# Redis (AI Worker 연동)
# ==============================
# worker.py와 동일한 Redis 주소 및 큐 이름 사용
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "video_processing_jobs")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

# ==============================
# 모델 (Request Body)
# ==============================
class GenerateRequest(BaseModel):
    prompt: str

class YoutubeUploadRequest(BaseModel):
    video_key: str
    title: str
    description: Optional[str] = None

# ==============================
# 1. 비디오 생성 요청 (KIE -> S3 -> Redis)
# ==============================
@router.post("/generate")
async def generate_video(
    req: GenerateRequest,
    token_payload: dict = Depends(verify_jwt),
):
    user_id = token_payload["sub"]
    if not KIE_API_KEY:
        raise HTTPException(500, "Server definition error: KIE_API_KEY missing")

    # 1. KIE API 호출 (Text-to-Video)
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                KIE_API_URL,
                headers={"Authorization": f"Bearer {KIE_API_KEY}"},
                json={"prompt": req.prompt},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"KIE API Error: {e}")
        raise HTTPException(502, f"KIE Generation failed: {e}")

    # KIE 응답 파싱 (구조에 따라 다를 수 있음, 예시: id, video_url)
    task_id = data.get("id") or f"kie_{os.urandom(4).hex()}"
    video_url = data.get("video_url")

    if not video_url:
        raise HTTPException(502, "KIE did not return a video URL")

    # 2. 영상 다운로드 (임시 파일)
    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
    try:
        async with httpx.AsyncClient() as client:
            v_resp = await client.get(video_url)
            if v_resp.status_code != 200:
                raise Exception("Failed to download video from KIE")
            with open(tmp_video, "wb") as f:
                f.write(v_resp.content)

        # 3. 썸네일 생성 (FFmpeg)
        tmp_thumb = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg").name
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", tmp_video,
                "-ss", "00:00:01",
                "-vframes", "1",
                tmp_thumb
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # 4. S3 업로드
        upload_video(user_id, task_id, tmp_video, processed=False)
        upload_thumbnail(user_id, task_id, tmp_thumb)

        # 5. DB 기록
        await insert_final_video(
            video_key=task_id,
            user_id=user_id,
            title=req.prompt[:50],  # 프롬프트 앞부분을 제목으로
            description=req.prompt,
        )

        # 6. Redis 작업 큐에 추가 (AI Worker가 처리하도록)
        # worker.py가 기대하는 메시지 포맷: {"input_key": "...", "output_key": "...", "variant": "..."}
        job_payload = {
            "input_key": f"{user_id}/{task_id}.mp4",
            "output_key": f"{user_id}/{task_id}_processed.mp4",
            "variant": "v1"
        }
        redis_client.lpush(REDIS_QUEUE, json.dumps(job_payload))

        await insert_operation_log(
            user_id=user_id,
            log_type="VIDEO_GENERATE",
            status="SUCCESS",
            video_key=task_id,
            message="KIE video generated and queued for AI worker"
        )

        return {"task_id": task_id, "status": "queued"}

    except Exception as e:
        await insert_operation_log(
            user_id=user_id,
            log_type="VIDEO_GENERATE",
            status="FAIL",
            message=str(e)
        )
        raise HTTPException(500, f"Processing failed: {e}")

    finally:
        # 임시 파일 정리
        if os.path.exists(tmp_video):
            os.remove(tmp_video)
        if 'tmp_thumb' in locals() and os.path.exists(tmp_thumb):
            os.remove(tmp_thumb)


# ==============================
# 2. 내 비디오 목록
# ==============================
@router.get("/list")
def get_my_videos(token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    # S3에서 목록 조회
    videos = list_user_videos(user_id)
    return {"videos": videos}


# ==============================
# 3. 스트리밍 (S3 -> Client)
# ==============================
@router.get("/stream/{task_id}")
def stream_video(
    task_id: str,
    processed: bool = Query(False),
    token_payload: dict = Depends(verify_jwt)
):
    user_id = token_payload["sub"]
    try:
        # S3 Body 스트림 반환
        file_stream = get_video_stream(user_id, task_id, processed)
        return StreamingResponse(file_stream, media_type="video/mp4")
    except Exception:
        raise HTTPException(404, "Video not found")

@router.get("/thumbnail/{task_id}")
def stream_thumbnail(
    task_id: str,
    token_payload: dict = Depends(verify_jwt)
):
    user_id = token_payload["sub"]
    try:
        file_stream = get_thumbnail_stream(user_id, task_id)
        return StreamingResponse(file_stream, media_type="image/jpeg")
    except Exception:
        raise HTTPException(404, "Thumbnail not found")


# ==============================
# 4. 유튜브 업로드
# ==============================
@router.post("/youtube/upload")
async def upload_to_youtube_api(
    body: YoutubeUploadRequest,
    token_payload: dict = Depends(verify_jwt),
):
    user_id = token_payload["sub"]
    task_id = body.video_key

    # 임시 파일로 다운로드 (S3 -> Local)
    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name

    try:
        # 1. 처리된 영상(processed) 우선 다운로드, 없으면 원본
        try:
            stream = get_video_stream(user_id, task_id, processed=True)
        except:
            stream = get_video_stream(user_id, task_id, processed=False)
        
        with open(tmp_video, "wb") as f:
            f.write(stream.read())

        # 2. 구글 인증 객체 생성
        youtube = get_youtube_service(user_id)

        # 3. 유튜브 API 호출
        request = youtube.videos().insert(
            part="snippet,status",
            body={
                "snippet": {
                    "title": body.title,
                    "description": body.description or f"Generated by Justic AI\nTask ID: {task_id}",
                    "categoryId": "22", # People & Blogs
                },
                "status": {"privacyStatus": "private"}, # 기본 비공개
            },
            media_body=MediaFileUpload(tmp_video, mimetype="video/mp4", resumable=True),
        )

        response = request.execute()
        youtube_id = response.get("id")

        if youtube_id:
            await mark_youtube_uploaded(
                video_key=task_id,
                youtube_video_id=youtube_id,
            )

        await insert_operation_log(
            user_id=user_id,
            log_type="YOUTUBE_UPLOAD",
            status="SUCCESS" if youtube_id else "UNKNOWN",
            video_key=task_id,
            message=f"YouTube upload finished (id={youtube_id})",
        )

        return {"status": "UPLOADED", "youtube_video_id": youtube_id}

    except Exception as e:
        await insert_operation_log(
            user_id=user_id,
            log_type="YOUTUBE_UPLOAD",
            status="FAIL",
            video_key=task_id,
            message=f"YouTube upload failed: {repr(e)}",
        )
        raise HTTPException(500, f"YouTube upload failed: {e}")

    finally:
        if os.path.exists(tmp_video):
            os.remove(tmp_video)