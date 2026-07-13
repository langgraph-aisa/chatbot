import asyncio
import hashlib
import json
import logging
import os
from collections import defaultdict
from typing import Any, AsyncGenerator

from langchain_core.messages import AIMessage, HumanMessage

from services.audio_service import AudioTranscriptionService
from telemetry import trace_id_var


logger = logging.getLogger("jarvi.api")
MAX_RECORD_MESSAGES = int(os.getenv("MAX_N8N_RECORD_MESSAGES", "20"))


def obtener_caso(thread_id: str) -> str:
    normalized = (thread_id or "").replace("-", "")
    return normalized[-12:] if normalized else "000000000000"


class ChatService:
    def __init__(self, graph: Any, audio_service: AudioTranscriptionService | None = None):
        self.graph = graph
        self.audio_service = audio_service or AudioTranscriptionService()
        self.locks = defaultdict(asyncio.Lock)

    async def resolver_mensaje_entrada(self, mensaje: str, url_n8n_audio: str | None = None) -> str:
        audio_url = (url_n8n_audio or "").strip()
        if audio_url:
            transcripcion = await self.audio_service.transcribe_audio_url(audio_url)
            logger.info("Audio n8n transcrito para /chat: %s caracteres", len(transcripcion))
            return transcripcion

        return (mensaje or "").strip()

    def _record_to_messages(self, thread_id: str, record: list[dict[str, Any]] | None, mensaje: str) -> list:
        if not record:
            return []

        messages = []
        for index, item in enumerate(record[-MAX_RECORD_MESSAGES:]):
            role = str(
                item.get("role")
                or item.get("type")
                or item.get("sender_type")
                or item.get("sender")
                or ""
            ).lower()
            content = (
                item.get("content")
                or item.get("text")
                or item.get("message")
                or item.get("body")
                or item.get("value")
                or ""
            )

            if isinstance(content, (dict, list)):
                content = json.dumps(content, ensure_ascii=False)
            content = str(content).strip()
            if not content:
                continue

            if item.get("fromMe") is True:
                role = "assistant"
            elif item.get("fromMe") is False and not role:
                role = "user"

            digest = hashlib.sha1(f"{role}:{content}".encode("utf-8")).hexdigest()[:16]
            message_id = f"n8n-record-{thread_id}-{index}-{digest}"

            if role in {"assistant", "ai", "bot", "jarvi"}:
                messages.append(AIMessage(content=content, id=message_id))
            elif role in {"user", "human", "cliente", "customer", "contact"}:
                messages.append(HumanMessage(content=content, id=message_id))

        if messages and isinstance(messages[-1], HumanMessage) and messages[-1].content.strip() == mensaje.strip():
            messages.pop()

        return messages

    async def _checkpoint_tiene_historial(self, config: dict) -> bool:
        aget_state = getattr(self.graph, "aget_state", None)
        if not aget_state:
            return False

        try:
            snapshot = await aget_state(config)
        except Exception as exc:
            logger.warning("No se pudo inspeccionar checkpoint antes de aplicar record: %s", exc)
            return False

        values = getattr(snapshot, "values", None) or {}
        return bool(values.get("messages"))

    async def generar_tokens(
        self,
        thread_id: str,
        mensaje: str,
        record: list[dict[str, Any]] | None = None,
        chat_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        contexto_inicial: dict[str, Any] | None = None,
    ) -> AsyncGenerator[str, None]:
        trace_id = trace_id_var.get()
        caso = obtener_caso(thread_id)
        chat_identifier = (chat_id or thread_id).strip()
        metadata_config = {
            "trace_id": trace_id,
            "chat_id": chat_identifier,
            "caso": caso,
            "origen": "n8n",
        }
        if metadata:
            metadata_config.update({k: v for k, v in metadata.items() if v not in (None, "")})

        config = {
            "configurable": {"thread_id": thread_id},
            "metadata": metadata_config,
            "run_name": caso,
        }

        async with self.locks[thread_id]:
            record_messages = []
            if record:
                if await self._checkpoint_tiene_historial(config):
                    logger.info("Record externo omitido: LangGraph ya tiene historial para thread_id=%s", thread_id)
                else:
                    record_messages = self._record_to_messages(thread_id, record, mensaje)

            estado_inicial = {
                "messages": [*record_messages, HumanMessage(content=mensaje)],
                "contexto_tecnico": dict(contexto_inicial or {}),
            }

            logger.info(
                "Ejecutando chat para thread_id=%s, trace_id=%s, record_messages=%s",
                thread_id,
                trace_id,
                len(record_messages),
            )

            resultado = await self.graph.ainvoke(estado_inicial, config=config)
            messages = resultado.get("messages", [])
            ctx = resultado.get("contexto_tecnico", {})

            logger.info("Contexto final para thread %s: %s", thread_id, ctx)

            respuesta_final = ""
            for msg in reversed(messages):
                if isinstance(msg, AIMessage):
                    respuesta_final = msg.content
                    break

            if respuesta_final:
                tokens = respuesta_final.split()
                for i, token in enumerate(tokens):
                    sep = " " if i < len(tokens) - 1 else ""
                    yield f"data: {json.dumps({'token': token + sep}, ensure_ascii=False)}\n\n"
            else:
                fallback = "No se pudo generar una respuesta. Por favor, intenta de nuevo."
                yield f"data: {json.dumps({'token': fallback}, ensure_ascii=False)}\n\n"

            ctx_para_envio = dict(ctx)
            ctx_para_envio.update(
                {
                    "chat_id": chat_identifier,
                    "thread_id": thread_id,
                    "caso": caso,
                    "run_name_actual": config["run_name"],
                    "origen": metadata_config.get("origen", "n8n"),
                    "fingerprint": metadata_config.get("fingerprint"),
                    "historial_externo_count": len(record or []),
                }
            )
            yield f"data: {json.dumps({'contexto_tecnico': ctx_para_envio}, ensure_ascii=False)}\n\n"
