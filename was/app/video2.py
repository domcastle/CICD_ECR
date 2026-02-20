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

# ✅ 라우터 태그 video2 유지
router = APIRouter(tags=["video2"])

# ==============================
# 환경 설정
# ==============================
# ✅ Grok 전용 엔드포인트 및 모델 설정
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
    variant: str = "v1" # ✅ 추가: 유튜브에 올릴 영상 버전 (기본값 v1)

# ==============================
# 1. 비디오 생성 요청 (Grok API 호출)
# ==============================
@router.post("/generate")
async def generate_video_v2(req: GenerateRequest, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    if not KIE_API_KEY:
        raise HTTPException(500, "KIE_API_KEY missing")

    payload = {
        "prompt": req.prompt,
        "model": "grok-imagine", # ✅ Grok 모델 사용
        "callBackUrl": f"{APP_BASE_URL}/api/video2/callback", # ✅ video2 콜백 경로
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
        print(f"KIE V2 API Error: {e}")
        raise HTTPException(502, f"KIE V2 Generation failed: {e}")

    # ✅ AttributeError: 'NoneType' object has no attribute 'get' 방지 로직
    if not data or not isinstance(data, dict):
        raise HTTPException(502, f"Invalid API response: {data}")

    data_block = data.get("data")
    if isinstance(data_block, dict):
        task_id = data_block.get("taskId")
    else:
        task_id = data.get("id")

    if not task_id:
        raise HTTPException(502, f"KIE V2 did not return taskId. Raw response: {data}")

    redis_client.set(f"task_user:{task_id}", user_id, ex=86400)
    redis_client.set(f"task_prompt:{task_id}", req.prompt, ex=86400)
    redis_client.set(f"task_status:{task_id}", "QUEUED", ex=86400)

    return {"task_id": task_id, "status": "QUEUED"}

# ==============================
# 1.5. 프론트 polling용 상태 조회
# ==============================
@router.get("/status/{task_id}")
def get_status_v2(task_id: str, token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    owner = redis_client.get(f"task_user:{task_id}")
    if not owner or owner != user_id:
        raise HTTPException(403, "Forbidden or Not Found")

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
    video_url = data.get("info", {}).get("resultUrls", [None])[0]

    if not task_id or not video_url:
        return {"code": 200, "msg": "waiting"}

    redis_client.set(f"task_status:{task_id}", "PROCESSING", ex=86400)
    user_id = redis_client.get(f"task_user:{task_id}")
    prompt = redis_client.get(f"task_prompt:{task_id}") or "Grok Video"

    if not user_id:
        redis_client.set(f"task_status:{task_id}", "FAILED", ex=86400)
        return {"code": 200, "msg": "User mapping not found"}

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

        # ✅ 원본 업로드 (variant=None 사용)
        upload_video(user_id, task_id, tmp_video)
        upload_thumbnail(user_id, task_id, tmp_thumb)

        await insert_final_video(
            video_key=task_id, user_id=user_id, title=prompt[:50], description=prompt
        )

        # ✅ Worker에게 작업 전달 (v1, v2 생성을 위해)
        job_payload = {
            "input_key": f"{user_id}/{task_id}.mp4",
            "output_key": f"{user_id}/{task_id}_processed.mp4",
        }
        redis_client.lpush(REDIS_QUEUE, json.dumps(job_payload))

        await insert_operation_log(
            user_id=user_id,
            log_type="VIDEO_GENERATE_V2", # ✅ V2 전용 로그 타입
            status="SUCCESS",
            video_key=task_id,
            message="Callback processed successfully"
        )
        redis_client.set(f"task_status:{task_id}", "COMPLETED", ex=86400)

    except Exception as e:
        print(f"V2 Callback error: {e}")
        redis_client.set(f"task_status:{task_id}", "FAILED", ex=86400)
        try:
            await insert_operation_log(
                user_id=user_id, log_type="VIDEO_GENERATE_V2", status="FAILED",
                video_key=task_id, message=str(e)
            )
        except Exception: pass
    finally:
        for f in (tmp_video, tmp_thumb):
            if os.path.exists(f): os.remove(f)

    return {"code": 200, "msg": "success"}

# ==============================
# 3. 내 비디오 목록
# ==============================
@router.get("/list")
def get_my_videos_v2(token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    # ✅ s3_client의 list_user_videos 호출 (중복 제거 없는 개별 노출 방식)
    videos = list_user_videos(user_id)
    return {"videos": videos}

# ==============================
# 4. 스트리밍 및 썸네일
# ==============================
@router.get("/stream/{task_id}")
def stream_video_v2(task_id: str, variant: Optional[str] = Query(None), token_payload: dict = Depends(verify_jwt)):
    user_id = token_payload["sub"]
    try:
        # ✅ variant 파라미터 전달 (v1, v2 등)
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
        # ✅ 사용자가 요청한 variant를 우선적으로 가져오기 시도
        try:
            stream = get_video_stream(user_id, task_id, variant=body.variant)
        except Exception:
            # 실패 시 원본 시도
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
        youtube_id = response.get("id")
        if youtube_id:
            await mark_youtube_uploaded(video_key=task_id, youtube_video_id=youtube_id)

        return {"status": "UPLOADED", "youtube_video_id": youtube_id}
    except Exception as e:
        raise HTTPException(500, f"YouTube upload failed: {e}")
    finally:
        if os.path.exists(tmp_video): os.remove(tmp_video)