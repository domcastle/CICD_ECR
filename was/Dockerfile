FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# =========================
# 시스템 패키지 + CA 설치 1
# =========================
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libpq-dev \
    gcc \
    curl \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# =========================
# AWS RDS CA 직접 추가
# =========================
RUN curl https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
    -o /etc/ssl/certs/rds-ca.pem

# =========================
# Python 의존성 설치
# =========================
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# =========================
# 소스 복사
# =========================
COPY . .

# =========================
# OpenShift 권한 대응
# =========================
RUN chgrp -R 0 /app && \
    chmod -R g=u /app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
