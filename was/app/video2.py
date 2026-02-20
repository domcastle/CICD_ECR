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

router = APIRouter(tags=["video2"])

# ==============================
# 환경 설정
# ==============================
KIE_CREATE_URL = "https://api.kie.ai/api/v1/jobs/createTask"
KIE_API_KEY = os.getenv("KIE_API_KEY")
APP_BASE_URL = os.getenv("APP_BASE_URL", "https://auth.justic.store")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("APP_REDIS_PORT", "6379"))
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
    variant: str = "v1" # ✅ 다중 버전 대응을 위해 추가 (기본값 v1)

# ==============================
# 1. 비디오 생성 요청 (Grok 모델)
# ==============================
@router.post("/generate")
async def generate_video_v2(req: GenerateRequest, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    if not KIE_API_KEY:
        raise HTTPException(500, "KIE_API_KEY missing")

    payload = {
        "prompt": req.prompt,
        "model": "grok-imagine",
        "callBackUrl": f"{APP_BASE_URL}/api/video2/callback"
    }

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                KIE_CREATE_URL,
                headers={"Authorization": f"Bearer {KIE_API_KEY}"},
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        raise HTTPException(502, f"KIE V2 Generation failed: {e}")

    task_id = data.get("data", {}).get("taskId") or data.get("id")
    if not task_id:
        raise HTTPException(502, "KIE V2 did not return a taskId")

    redis_client.set(f"task_user:{task_id}", user_id, ex=86400)
    redis_client.set(f"task_prompt:{task_id}", req.prompt, ex=86400)
    redis_client.set(f"task_status:{task_id}", "QUEUED", ex=86400)

    return {"task_id": task_id, "status": "QUEUED"}

# ==============================
# 1.5. Status API
# ==============================
@router.get("/status/{task_id}")
def get_status_v2(task_id: str, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]

    owner = redis_client.get(f"task_user:{task_id}")
    if not owner:
        return {"task_id": task_id, "status": "NOT_FOUND"}
    if owner != user_id:
        raise HTTPException(403, "Forbidden")

    status = redis_client.get(f"task_status:{task_id}") or "UNKNOWN"
    return {"task_id": task_id, "status": status}

# ==============================
# 2. 콜백 수신부
# ==============================
@router.post("/callback")
async def video2_callback(request: Request):
    payload = await request.json()

    data = payload.get("data", {})
    task_id = data.get("taskId")
    video_url = data.get("info", {}).get("resultUrls", [None])[0]

    if not task_id or not video_url:
        return {"code": 200, "msg": "waiting"}

    redis_client.set(f"task_status:{task_id}", "PROCESSING", ex=86400)

    user_id = redis_client.get(f"task_user:{task_id}")
    prompt = redis_client.get(f"task_prompt:{task_id}") or "Grok Video"

    if not user_id:
        redis_client.set(f"task_status:{task_id}", "FAILED", ex=86400)
        return {"code": 200, "msg": "User not found"}

    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
    tmp_thumb = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg").name

    try:
        async with httpx.AsyncClient(timeout=300) as client:
            v_resp = await client.get(video_url)
            v_resp.raise_for_status()
            with open(tmp_video, "wb") as f:
                f.write(v_resp.content)

        subprocess.run(
            ["ffmpeg", "-y", "-i", tmp_video, "-ss", "00:00:01", "-vframes", "1", tmp_thumb],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

        # ✅ 수정: 원본 업로드 시 처리 여부(processed) 제외하고 variant=None 사용
        upload_video(user_id, task_id, tmp_video)
        upload_thumbnail(user_id, task_id, tmp_thumb)

        try:
            await insert_final_video(
                video_key=task_id,
                user_id=user_id,
                title=prompt[:50],
                description=prompt,
            )
        except Exception as db_e:
            print(f"DB Error (V2): {db_e}")

        # ✅ 수정: worker.py가 알아서 v1, v2 둘 다 생성하므로 variant 속성 제거
        job_payload = {
            "input_key": f"{user_id}/{task_id}.mp4",
            "output_key": f"{user_id}/{task_id}_processed.mp4",
        }
        redis_client.lpush(REDIS_QUEUE, json.dumps(job_payload))

        await insert_operation_log(
            user_id=user_id,
            log_type="VIDEO_GENERATE_V2",
            status="SUCCESS",
            video_key=task_id,
            message="KIE V2 callback processed"
        )

        redis_client.set(f"task_status:{task_id}", "COMPLETED", ex=86400)

    except Exception as e:
        print(f"V2 Callback processing error: {e}")
        redis_client.set(f"task_status:{task_id}", "FAILED", ex=86400)
        try:
            await insert_operation_log(
                user_id=user_id,
                log_type="VIDEO_GENERATE_V2",
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
# 3. 목록 및 스트리밍
# ==============================
@router.get("/list")
def get_my_videos_v2(token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    videos = list_user_videos(user_id)
    return {"videos": videos}

# ✅ 수정: processed 대신 variant 쿼리 파라미터로 처리
@router.get("/stream/{task_id}")
def stream_video_v2(task_id: str, variant: Optional[str] = Query(None), token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    try:
        file_stream = get_video_stream(user_id, task_id, variant=variant)
        return StreamingResponse(file_stream, media_type="video/mp4")
    except Exception:
        raise HTTPException(404, "Video not found")

@router.get("/thumbnail/{task_id}")
def stream_thumbnail_v2(task_id: str, token_payload: dict = Depends(verify_jwt)):
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
async def upload_to_youtube_v2(body: YoutubeUploadRequest, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    task_id = body.video_key
    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name

    try:
        # ✅ 수정: body.variant 로 시도하고 없으면 원본으로 폴백
        try:
            stream = get_video_stream(user_id, task_id, variant=body.variant)
        except Exception:
            stream = get_video_stream(user_id, task_id, variant=None)

        with open(tmp_video, "wb") as f:
            f.write(stream.read())

        youtube = get_youtube_service(user_id)
        request = youtube.videos().insert(
            part="snippet,status",
            body={
                "snippet": {
                    "title": body.title,
                    "description": body.description or f"Grok Video: {task_id}",
                    "categoryId": "22",
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
        raise HTTPException(500, f"Upload failed: {e}")
    finally:
        if os.path.exists(tmp_video):
            os.remove(tmp_video)