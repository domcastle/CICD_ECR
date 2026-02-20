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
KIE_API_URL = "https://api.kie.ai/api/v1/jobs/createTask"
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
    variant: str = "v1"

# ==============================
# 1. 비디오 생성 요청 (Grok API 규격 적용)
# ==============================
@router.post("/generate")
async def generate_video_v2(req: GenerateRequest, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    if not KIE_API_KEY:
        raise HTTPException(500, "KIE_API_KEY missing")

    # ✅ Grok Imagine 공식 Payload 구조
    payload = {
        "model": "grok-imagine/text-to-video",
        "callBackUrl": f"{APP_BASE_URL}/api/video2/callback",
        "input": {
            "prompt": req.prompt,
            "aspect_ratio": "9:16",
            "mode": "normal",
            "duration": "6",
            "resolution": "480p"
        }
    }

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                KIE_API_URL,
                headers={"Authorization": f"Bearer {KIE_API_KEY}"},
                json=payload
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"Grok API Request Error: {e}")
        raise HTTPException(502, f"Grok API call failed: {e}")

    # ✅ 문서 규격에 따른 taskId 추출 (data.taskId)
    task_id = data.get("data", {}).get("taskId")
    if not task_id:
        raise HTTPException(502, f"Grok did not return taskId: {data}")

    redis_client.set(f"task_user:{task_id}", user_id, ex=86400)
    redis_client.set(f"task_prompt:{task_id}", req.prompt, ex=86400)
    redis_client.set(f"task_status:{task_id}", "QUEUED", ex=86400)

    return {"task_id": task_id, "status": "QUEUED"}

# ==============================
# 1.5. 상태 조회 (Polling)
# ==============================
@router.get("/status/{task_id}")
def get_status_v2(task_id: str, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    owner = redis_client.get(f"task_user:{task_id}")
    if not owner or owner != user_id:
        raise HTTPException(403, "Forbidden")

    status = redis_client.get(f"task_status:{task_id}") or "UNKNOWN"
    return {"task_id": task_id, "status": status}

# ==============================
# 2. 비디오 생성 완료 콜백 (Grok -> WAS)
# ==============================
@router.post("/callback")
async def video2_callback(request: Request):
    payload = await request.json()
    data = payload.get("data", {})
    task_id = data.get("taskId")
    
    # Grok 결과 URL 추출 (info.resultUrls 또는 videoUrl 대응)
    video_url = data.get("info", {}).get("resultUrls", [None])[0] or data.get("videoUrl")

    if not task_id or not video_url:
        return {"code": 200, "msg": "waiting"}

    user_id = redis_client.get(f"task_user:{task_id}")
    prompt = redis_client.get(f"task_prompt:{task_id}") or "Grok Video"

    if not user_id:
        return {"code": 200, "msg": "User mapping not found"}

    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
    tmp_thumb = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg").name

    try:
        async with httpx.AsyncClient(timeout=300) as client:
            v_resp = await client.get(video_url)
            with open(tmp_video, "wb") as f:
                f.write(v_resp.content)

        # 썸네일 생성
        subprocess.run(
            ["ffmpeg", "-y", "-i", tmp_video, "-ss", "00:00:01", "-vframes", "1", tmp_thumb],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

        # S3 업로드 (원본)
        upload_video(user_id, task_id, tmp_video)
        upload_thumbnail(user_id, task_id, tmp_thumb)

        # DB 기록
        await insert_final_video(video_key=task_id, user_id=user_id, title=prompt[:50])

        # ✅ AI 워커에게 v1, v2 다중 생성 작업 전달
        job_payload = {
            "input_key": f"{user_id}/{task_id}.mp4",
            "output_key": f"{user_id}/{task_id}_processed.mp4",
        }
        redis_client.lpush(REDIS_QUEUE, json.dumps(job_payload))
        
        redis_client.set(f"task_status:{task_id}", "COMPLETED", ex=86400)
        await insert_operation_log(user_id, "VIDEO_GENERATE_V2", "SUCCESS", task_id)

    except Exception as e:
        print(f"Grok Callback Error: {e}")
        redis_client.set(f"task_status:{task_id}", "FAILED", ex=86400)
    finally:
        for f in (tmp_video, tmp_thumb):
            if os.path.exists(f): os.remove(f)

    return {"code": 200, "msg": "success"}

# ==============================
# 3. 내 비디오 목록 (개별 노출 방식)
# ==============================
@router.get("/list")
def get_my_videos_v2(token_payload: dict = Depends(verify_jwt)):
    return {"videos": list_user_videos(token_payload["sub"])}

# ==============================
# 4. 스트리밍 및 썸네일
# ==============================
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
# 5. 유튜브 업로드
# ==============================
@router.post("/youtube/upload")
async def upload_to_youtube_api_v2(body: YoutubeUploadRequest, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    task_id = body.video_key
    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
    try:
        # 요청 버전(v1, v2) 우선 시도, 실패 시 원본 시도
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
                "snippet": {"title": body.title, "categoryId": "22"},
                "status": {"privacyStatus": "private"},
            },
            media_body=MediaFileUpload(tmp_video, mimetype="video/mp4", resumable=True),
        )
        response = request.execute()
        return {"status": "UPLOADED", "youtube_video_id": response.get("id")}
    except Exception as e:
        raise HTTPException(500, f"YouTube upload failed: {e}")
    finally:
        if os.path.exists(tmp_video): os.remove(tmp_video)