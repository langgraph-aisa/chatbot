import asyncio
import json
import logging
from collections import defaultdict
from typing import Any, AsyncGenerator

from langchain_core.messages import AIMessage, HumanMessage

from services.audio_service import AudioTranscriptionService
from telemetry import trace_id_var


logger = logging.getLogger("jarvi.api")


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

    async def generar_tokens(self, thread_id: str, mensaje: str) -> AsyncGenerator[str, None]:
        trace_id = trace_id_var.get()
        config = {"configurable": {"thread_id": thread_id}, "metadata": {"trace_id": trace_id}}
        estado_inicial = {
            "messages": [HumanMessage(content=mensaje)],
            "contexto_tecnico": {},
        }

        logger.info("Ejecutando chat para thread_id=%s, trace_id=%s", thread_id, trace_id)

        async with self.locks[thread_id]:
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

            yield f"data: {json.dumps({'contexto_tecnico': ctx}, ensure_ascii=False)}\n\n"
