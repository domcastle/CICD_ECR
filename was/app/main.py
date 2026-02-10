from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from auth import router as auth_router
from video import router as video_router
from video2 import router as video2_router
from health import router as health_router

# MinIO 관련 import 삭제

app = FastAPI(
    title="Justic API Server",
    version="3.6", # 버전 업
)

# Startup 이벤트 삭제 (S3는 ensure_bucket 불필요)

# =========================
# CORS 설정
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://justic.store:8000",
        "http://justic.store",
        "https://justic.store",
        "http://auth.justic.store:8000",
        "http://auth.justic.store",
        "https://auth.justic.store:8000",
        "https://auth.justic.store",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# Router 등록
# =========================
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(video_router, prefix="/api/video", tags=["video"])
app.include_router(video2_router, prefix="/api/video2", tags=["video2"])
app.include_router(health_router, prefix="/health", tags=["health"])

@app.get("/")
def root():
    return {"status": "ok"}