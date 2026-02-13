import os
import json
import httpx
import subprocess
import tempfile
import redis
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from app.security import verify_jwt
from app.s3_client import (
    upload_video,
    upload_thumbnail,
    get_video_stream,
    get_thumbnail_stream,
    list_user_videos,
)
from app.ai import (
    insert_final_video,
    mark_youtube_uploaded,
    insert_operation_log,
)
from app.google_auth import get_youtube_service
from googleapiclient.http import MediaFileUpload

router = APIRouter(tags=["video"])

# ==============================
# 환경 설정
# ==============================
KIE_API_URL = "https://api.kie.ai/api/v1/veo/generate"
KIE_API_KEY = os.getenv("KIE_API_KEY")
APP_BASE_URL = os.getenv("APP_BASE_URL", "https://auth.justic.store")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("APP_REDIS_PORT", "6379"))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "video_processing_jobs")

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

class GenerateRequest(BaseModel):
    prompt: str

class YoutubeUploadRequest(BaseModel):
    video_key: str
    title: str
    description: Optional[str] = None

# ==============================
# 1. 비디오 생성 요청 (KIE API 호출)
# ==============================
@router.post("/generate")
async def generate_video(req: GenerateRequest, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    if not KIE_API_KEY:
        raise HTTPException(500, "KIE_API_KEY missing")

    payload = {
        "prompt": req.prompt,
        "model": "veo3_fast",
        "aspect_ratio": "9:16",
        "callBackUrl": f"{APP_BASE_URL}/api/video/callback",
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                KIE_API_URL,
                headers={"Authorization": f"Bearer {KIE_API_KEY}"},
                json=payload
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"KIE API Error: {e}")
        raise HTTPException(502, f"KIE Generation failed: {e}")

    task_id = data.get("data", {}).get("taskId")
    if not task_id:
        raise HTTPException(502, "KIE did not return taskId")

    # 콜백/상태 조회용 Redis 저장
    redis_client.set(f"task_user:{task_id}", user_id, ex=86400)
    redis_client.set(f"task_prompt:{task_id}", req.prompt, ex=86400)
    redis_client.set(f"task_status:{task_id}", "QUEUED", ex=86400)

    return {"task_id": task_id, "status": "QUEUED"}

# ==============================
# ✅ 1.5. 프론트 polling용 상태 조회 (추가)
# ==============================
@router.get("/status/{task_id}")
def get_status(task_id: str, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]

    owner = redis_client.get(f"task_user:{task_id}")
    if not owner:
        return {"task_id": task_id, "status": "NOT_FOUND"}
    if owner != user_id:
        # 다른 유저가 만든 task_id 조회 방지
        raise HTTPException(403, "Forbidden")

    status = redis_client.get(f"task_status:{task_id}") or "UNKNOWN"
    return {"task_id": task_id, "status": status}

# ==============================
# 2. 비디오 생성 완료 콜백 (KIE -> WAS)
# ==============================
@router.post("/callback")
async def video_callback(request: Request):
    payload = await request.json()

    data = payload.get("data", {})
    task_id = data.get("taskId")
    video_url = data.get("info", {}).get("resultUrls", [None])[0]

    if not task_id or not video_url:
        return {"code": 200, "msg": "waiting"}

    # 상태 업데이트
    redis_client.set(f"task_status:{task_id}", "PROCESSING", ex=86400)

    user_id = redis_client.get(f"task_user:{task_id}")
    prompt = redis_client.get(f"task_prompt:{task_id}") or "Generated Video"

    if not user_id:
        redis_client.set(f"task_status:{task_id}", "FAILED", ex=86400)
        return {"code": 200, "msg": "User mapping not found"}

    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
    tmp_thumb = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg").name

    try:
        # 영상 다운로드
        async with httpx.AsyncClient(timeout=300) as client:
            v_resp = await client.get(video_url)
            v_resp.raise_for_status()
            with open(tmp_video, "wb") as f:
                f.write(v_resp.content)

        # 썸네일 생성
        subprocess.run(
            ["ffmpeg", "-y", "-i", tmp_video, "-ss", "00:00:01", "-vframes", "1", tmp_thumb],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

        # S3 업로드
        upload_video(user_id, task_id, tmp_video, processed=False)
        upload_thumbnail(user_id, task_id, tmp_thumb)

        # DB 기록
        await insert_final_video(
            video_key=task_id,
            user_id=user_id,
            title=prompt[:50],
            description=prompt
        )

        # ✅ worker payload의 input_key 버그 수정 ("/.mp4" → ".mp4")
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
            message="Callback processed successfully"
        )

        redis_client.set(f"task_status:{task_id}", "COMPLETED", ex=86400)

    except Exception as e:
        print(f"Callback processing error: {e}")
        redis_client.set(f"task_status:{task_id}", "FAILED", ex=86400)
        try:
            await insert_operation_log(
                user_id=user_id,
                log_type="VIDEO_GENERATE",
                status="FAILED",
                video_key=task_id,
                message=str(e)
            )
        except Exception:
            pass
    finally:
        if os.path.exists(tmp_video): os.remove(tmp_video)
        if os.path.exists(tmp_thumb): os.remove(tmp_thumb)

    return {"code": 200, "msg": "success"}

# ==============================
# 3. 내 비디오 목록
# ==============================
@router.get("/list")
def get_my_videos(token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    videos = list_user_videos(user_id)
    return {"videos": videos}

# ==============================
# 4. 스트리밍 및 썸네일
# ==============================
@router.get("/stream/{task_id}")
def stream_video(task_id: str, processed: bool = Query(False), token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    try:
        file_stream = get_video_stream(user_id, task_id, processed)
        return StreamingResponse(file_stream, media_type="video/mp4")
    except Exception:
        raise HTTPException(404, "Video not found")

@router.get("/thumbnail/{task_id}")
def stream_thumbnail(task_id: str, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    try:
        file_stream = get_thumbnail_stream(user_id, task_id)
        return StreamingResponse(file_stream, media_type="image/jpeg")
    except Exception:
        raise HTTPException(404, "Thumbnail not found")

# ==============================
# 5. 유튜브 업로드
# ==============================
@router.post("/youtube/upload")
async def upload_to_youtube_api(body: YoutubeUploadRequest, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    task_id = body.video_key
    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
    try:
        try:
            stream = get_video_stream(user_id, task_id, processed=True)
        except Exception:
            stream = get_video_stream(user_id, task_id, processed=False)

        with open(tmp_video, "wb") as f:
            f.write(stream.read())

        youtube = get_youtube_service(user_id)
        request = youtube.videos().insert(
            part="snippet,status",
            body={
                "snippet": {
                    "title": body.title,
                    "description": body.description or f"Task: {task_id}",
                    "categoryId": "22"
                },
                "status": {"privacyStatus": "private"},
            },
            media_body=MediaFileUpload(tmp_video, mimetype="video/mp4", resumable=True),
        )
        response = request.execute()
        youtube_id = response.get("id")
        if youtube_id:
            await mark_youtube_uploaded(video_key=task_id, youtube_video_id=youtube_id)

        return {"status": "UPLOADED", "youtube_video_id": youtube_id}
    except Exception as e:
        raise HTTPException(500, f"YouTube upload failed: {e}")
    finally:
        if os.path.exists(tmp_video):
            os.remove(tmp_video)
