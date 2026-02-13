# db.py
import redis
import ssl
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.config import DB_URL, REDIS_HOST, REDIS_PORT, REDIS_DB

# =========================
# RDS SSL Context 생성
# =========================
ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations("/etc/ssl/certs/rds-ca.pem")

engine = create_async_engine(
    DB_URL,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"ssl": ssl_context}
)

AsyncSessionLocal = sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession
)

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True,
    socket_connect_timeout=2,
    socket_timeout=2,
    retry_on_timeout=True,
)
