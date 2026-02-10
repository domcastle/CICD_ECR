FROM python:3.10-slim

# 1. 로그 즉시 출력 (컨테이너 환경 필수)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 2. 필수 시스템 패키지 설치
# [cite_start]- ffmpeg: video.py에서 썸네일 생성 시 사용 [cite: 1]
# - libpq-dev, gcc: DB 드라이버 빌드용
# - curl: 헬스 체크용
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libpq-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 3. 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. 소스 코드 복사
COPY . .

# 5. [핵심] ROSA(OpenShift) 권한 문제 해결
# 임의의 UID로 실행되더라도 파일 접근이 가능하도록 그룹 권한 조정
RUN chgrp -R 0 /app && \
    chmod -R g=u /app

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]