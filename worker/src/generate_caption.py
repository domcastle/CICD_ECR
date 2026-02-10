import sys
import subprocess
import base64
import requests
import tempfile
import os
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

VARIANT = os.getenv("CAPTION_VARIANT", "v1")
PROMPT = PROMPTS.get(VARIANT, PROMPTS["v1"])

def ollama_chat(image_b64: str, timeout=120) -> str:
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": PROMPT,
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

def sanitize(text: str) -> str:
    for c in ["\n", "\r", "'", '"', "(", ")", "[", "]", "#", "*", ":", "."]:
        text = text.replace(c, "")
    return text.strip()

def main():
    if len(sys.argv) != 2:
        print(DEFAULT_TEXT)
        return

    video = Path(sys.argv[1])
    if not video.exists():
        print(DEFAULT_TEXT)
        return

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
        caption = sanitize(ollama_chat(img_b64))
        print(caption if caption else DEFAULT_TEXT)

    except Exception:
        print(DEFAULT_TEXT)

    finally:
        frame.unlink(missing_ok=True)

if __name__ == "__main__":
    main()