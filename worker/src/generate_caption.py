import sys
import subprocess
import base64
import requests
import tempfile
import os
import json
from pathlib import Path

MODEL = "qwen2.5vl"
# 환경변수에서 호스트 주소를 받아옴 (Hybrid 모드 핵심)
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
OLLAMA_URL = f"{OLLAMA_HOST}/api/chat"

DEFAULT_TEXT = "편집된 영상"

PROMPTS = {
    "v1": (
        "이 이미지를 보고 "
        "영상 썸네일에 쓸 짧은 한국어 제목을 만들어라. "
        "최대 15자. 설명 금지. 문장부호 금지."
    ),
    "v2": (
        "이 이미지를 보고 "
        "쇼츠 영상에 어울리는 강렬하고 눈에 띄는 한국어 제목을 만들어라. "
        "반드시 순수 한글만 사용하라. "
        "이모지, 특수문자, 전각문자, 영어, 숫자 절대 사용 금지. "
        "공백은 허용한다. "
        "최대 15자. 설명 금지. 문장부호 금지."
    ),
}

# 1. 특정 프롬프트를 매개변수로 받도록 수정됨
def ollama_chat(image_b64: str, prompt_text: str, timeout=120) -> str:
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": prompt_text,
                "images": [image_b64],
            }
        ],
        "stream": False,
    }

    try:
        r = requests.post(OLLAMA_URL, json=payload, timeout=timeout)
        r.raise_for_status()
        return (r.json().get("message", {}).get("content") or "").strip()
    except requests.exceptions.RequestException as e:
        sys.stderr.write(f"Ollama Request Error: {e}\n")
        return ""

# 2. 불필요한 특수문자 제거 로직
def sanitize(text: str) -> str:
    for c in ["\n", "\r", "'", '"', "(", ")", "[", "]", "#", "*", ":", "."]:
        text = text.replace(c, "")
    return text.strip()

def main():
    # 예외 발생 시 반환할 기본 JSON 구조
    default_json = json.dumps({"v1": DEFAULT_TEXT, "v2": DEFAULT_TEXT}, ensure_ascii=False)

    if len(sys.argv) != 2:
        print(default_json)
        return

    video = Path(sys.argv[1])
    if not video.exists():
        print(default_json)
        return

    # 3. 썸네일 프레임 추출
    fd, frame_path = tempfile.mkstemp(suffix=".jpg")
    os.close(fd)
    frame = Path(frame_path)

    try:
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-ss", "00:00:01",
                "-i", str(video),
                "-vf", "scale=320:-1",
                "-frames:v", "1",
                "-q:v", "10",
                str(frame),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        img_b64 = base64.b64encode(frame.read_bytes()).decode()
        
        # 4. v1, v2 프롬프트를 각각 순회하며 자막 생성
        captions = {}
        for variant, prompt in PROMPTS.items():
            result = sanitize(ollama_chat(img_b64, prompt))
            # 결과가 비어있으면 기본 텍스트 삽입
            captions[variant] = result if result else DEFAULT_TEXT

        # 5. JSON 형식으로 콘솔에 출력 (worker.py가 이 문자열을 읽음)
        print(json.dumps(captions, ensure_ascii=False))

    except Exception as e:
        sys.stderr.write(f"Error occurred: {e}\n")
        print(default_json)

    finally:
        # 임시 프레임 이미지 삭제
        frame.unlink(missing_ok=True)

if __name__ == "__main__":
    main()