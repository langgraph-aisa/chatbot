import asyncio
import io
import logging
import mimetypes
import os
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx
from openai import OpenAI


logger = logging.getLogger("jarvi.audio")

MAX_AUDIO_BYTES = int(os.getenv("MAX_N8N_AUDIO_BYTES", str(25 * 1024 * 1024)))
TRANSCRIPTION_MODEL = os.getenv("OPENAI_TRANSCRIPTION_MODEL", "gpt-4o-mini-transcribe")
TRANSCRIPTION_LANGUAGE = os.getenv("OPENAI_TRANSCRIPTION_LANGUAGE", "es").strip() or None

SUPPORTED_EXTENSIONS = {
    ".flac",
    ".m4a",
    ".mp3",
    ".mp4",
    ".mpeg",
    ".mpga",
    ".ogg",
    ".wav",
    ".webm",
}

CONTENT_TYPE_EXTENSIONS = {
    "audio/flac": ".flac",
    "audio/m4a": ".m4a",
    "audio/mp3": ".mp3",
    "audio/mp4": ".mp4",
    "audio/mpeg": ".mp3",
    "audio/mpga": ".mpga",
    "audio/ogg": ".ogg",
    "audio/wav": ".wav",
    "audio/webm": ".webm",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
}


class AudioTranscriptionError(RuntimeError):
    pass


class AudioTranscriptionService:
    def __init__(self, model: str | None = None):
        self.model = model or TRANSCRIPTION_MODEL

    async def transcribe_audio_url(self, audio_url: str) -> str:
        url = (audio_url or "").strip()
        if not url:
            return ""

        audio_bytes, filename, content_type = await self._download_audio(url)
        logger.info(
            "Audio descargado desde n8n: filename=%s, content_type=%s, bytes=%s",
            filename,
            content_type,
            len(audio_bytes),
        )
        return await asyncio.to_thread(self._transcribe_audio_bytes, audio_bytes, filename)

    async def _download_audio(self, audio_url: str) -> tuple[bytes, str, str]:
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            try:
                async with client.stream("GET", audio_url) as response:
                    response.raise_for_status()
                    content_type = response.headers.get("content-type", "").split(";")[0].strip().lower()
                    self._validate_content_length(response.headers.get("content-length"))

                    chunks: list[bytes] = []
                    total = 0
                    async for chunk in response.aiter_bytes():
                        total += len(chunk)
                        if total > MAX_AUDIO_BYTES:
                            raise AudioTranscriptionError(
                                f"El audio excede el limite de {MAX_AUDIO_BYTES // (1024 * 1024)} MB."
                            )
                        chunks.append(chunk)
            except httpx.HTTPStatusError as exc:
                raise AudioTranscriptionError(
                    f"No se pudo descargar el audio: HTTP {exc.response.status_code}."
                ) from exc
            except httpx.HTTPError as exc:
                raise AudioTranscriptionError("No se pudo descargar el audio desde la URL recibida.") from exc

        audio_bytes = b"".join(chunks)
        if not audio_bytes:
            raise AudioTranscriptionError("El archivo de audio descargado esta vacio.")

        filename = self._filename_from_url(audio_url, content_type)
        return audio_bytes, filename, content_type

    def _validate_content_length(self, content_length: str | None) -> None:
        if not content_length:
            return
        try:
            size = int(content_length)
        except ValueError:
            return
        if size > MAX_AUDIO_BYTES:
            raise AudioTranscriptionError(
                f"El audio excede el limite de {MAX_AUDIO_BYTES // (1024 * 1024)} MB."
            )

    def _filename_from_url(self, audio_url: str, content_type: str) -> str:
        path = unquote(urlparse(audio_url).path)
        filename = Path(path).name or "audio"
        extension = Path(filename).suffix.lower()

        if extension not in SUPPORTED_EXTENSIONS:
            extension = CONTENT_TYPE_EXTENSIONS.get(content_type)
            if not extension:
                guessed_extension = mimetypes.guess_extension(content_type or "")
                extension = guessed_extension if guessed_extension in SUPPORTED_EXTENSIONS else ".webm"
            filename = f"audio{extension}"

        return filename

    def _transcribe_audio_bytes(self, audio_bytes: bytes, filename: str) -> str:
        api_key = os.getenv("OPENAI_API_KEY1")
        if not api_key:
            raise AudioTranscriptionError("OPENAI_API_KEY1 no configurada para transcribir audio.")

        audio_file = io.BytesIO(audio_bytes)
        audio_file.name = filename

        client = OpenAI(api_key=api_key)
        transcription_kwargs = {
            "model": self.model,
            "file": audio_file,
        }
        if TRANSCRIPTION_LANGUAGE:
            transcription_kwargs["language"] = TRANSCRIPTION_LANGUAGE

        transcription = client.audio.transcriptions.create(**transcription_kwargs)
        text = getattr(transcription, "text", None)
        if text is None and isinstance(transcription, str):
            text = transcription

        text = (text or "").strip()
        if not text:
            raise AudioTranscriptionError("OpenAI no devolvio texto para el audio recibido.")

        return text
