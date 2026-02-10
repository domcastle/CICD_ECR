# app/video2.py
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

router = APIRouter(tags=["video2"])

# ==============================
# KIE - grok-imagine (Text -> Video)
# ==============================
KIE_CREATE_URL = "https://api.kie.ai/api/v1/jobs/createTask"
KIE_API_KEY = os.getenv("KIE_API_KEY")

# ==============================
# Redis (AI Worker 연동)
# ==============================
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "video_processing_jobs")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

class GenerateRequest(BaseModel):
    prompt: str

class YoutubeUploadRequest(BaseModel):
    video_key: str
    title: str
    description: Optional[str] = None

# ==============================
# 1. 비디오 생성 요청 (KIE V2 -> S3 -> Redis)
# ==============================
@router.post("/generate")
async def generate_video_v2(
    req: GenerateRequest,
    token_payload: dict = Depends(verify_jwt),
):
    user_id = token_payload["sub"]
    if not KIE_API_KEY:
        raise HTTPException(500, "KIE_API_KEY missing")

    # 1. KIE API 호출
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                KIE_CREATE_URL,
                headers={"Authorization": f"Bearer {KIE_API_KEY}"},
                json={"prompt": req.prompt, "model": "grok-imagine"},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        raise HTTPException(502, f"KIE V2 Generation failed: {e}")

    # 응답 파싱
    task_id = data.get("id") or f"kie2_{os.urandom(4).hex()}"
    video_url = data.get("video_url")

    if not video_url:
        raise HTTPException(502, "KIE V2 did not return a video URL")

    # 2. 영상 다운로드 (임시 파일)
    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
    try:
        async with httpx.AsyncClient() as client:
            v_resp = await client.get(video_url)
            if v_resp.status_code != 200:
                raise Exception("Failed to download video")
            with open(tmp_video, "wb") as f:
                f.write(v_resp.content)

        # 3. 썸네일 생성
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
            title=req.prompt[:50],
            description=req.prompt,
        )

        # 6. Redis 큐잉 (AI Worker 연동)
        job_payload = {
            "input_key": f"{user_id}/{task_id}.mp4",
            "output_key": f"{user_id}/{task_id}_processed.mp4",
            "variant": "v2"  # video2.py는 variant v2 사용
        }
        redis_client.lpush(REDIS_QUEUE, json.dumps(job_payload))

        await insert_operation_log(
            user_id=user_id,
            log_type="VIDEO_GENERATE_V2",
            status="SUCCESS",
            video_key=task_id,
            message="KIE V2 video generated and queued"
        )

        return {"task_id": task_id, "status": "queued"}

    except Exception as e:
        await insert_operation_log(
            user_id=user_id,
            log_type="VIDEO_GENERATE_V2",
            status="FAIL",
            message=str(e)
        )
        raise HTTPException(500, f"Processing failed: {e}")

    finally:
        if os.path.exists(tmp_video):
            os.remove(tmp_video)
        if 'tmp_thumb' in locals() and os.path.exists(tmp_thumb):
            os.remove(tmp_thumb)


# ==============================
# 2. 목록 및 스트리밍 (video.py와 동일 로직)
# ==============================
@router.get("/list")
def get_my_videos_v2(token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    videos = list_user_videos(user_id)
    return {"videos": videos}

@router.get("/stream/{task_id}")
def stream_video_v2(
    task_id: str,
    processed: bool = Query(False),
    token_payload: dict = Depends(verify_jwt)
):
    user_id = token_payload["sub"]
    try:
        file_stream = get_video_stream(user_id, task_id, processed)
        return StreamingResponse(file_stream, media_type="video/mp4")
    except Exception:
        raise HTTPException(404, "Video not found")

@router.get("/thumbnail/{task_id}")
def stream_thumbnail_v2(
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
# 3. 유튜브 업로드
# ==============================
@router.post("/youtube/upload")
async def upload_to_youtube_v2(
    body: YoutubeUploadRequest,
    token_payload: dict = Depends(verify_jwt),
):
    user_id = token_payload["sub"]
    task_id = body.video_key

    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name

    try:
        try:
            stream = get_video_stream(user_id, task_id, processed=True)
        except:
            stream = get_video_stream(user_id, task_id, processed=False)
        
        with open(tmp_video, "wb") as f:
            f.write(stream.read())

        youtube = get_youtube_service(user_id)

        request = youtube.videos().insert(
            part="snippet,status",
            body={
                "snippet": {
                    "title": body.title,
                    "description": body.description,
                    "categoryId": "22",
                },
                "status": {"privacyStatus": "private"},
            },
            media_body=MediaFileUpload(tmp_video, mimetype="video/mp4", resumable=True),
        )

        response = request.execute()
        youtube_id = response.get("id")

        if youtube_id:
            await mark_youtube_uploaded(task_id, youtube_id)

        await insert_operation_log(
            user_id=user_id,
            log_type="YOUTUBE_UPLOAD_V2",
            status="SUCCESS",
            video_key=task_id,
            message=f"ID: {youtube_id}",
        )
        return {"status": "UPLOADED", "youtube_video_id": youtube_id}

    except Exception as e:
        await insert_operation_log(
            user_id=user_id,
            log_type="YOUTUBE_UPLOAD_V2",
            status="FAIL",
            video_key=task_id,
            message=str(e),
        )
        raise HTTPException(500, f"Upload failed: {e}")
    finally:
        if os.path.exists(tmp_video):
            os.remove(tmp_video)