"""
api_server.py (backend central JARVI 2.0.03 + CTFOM)
=====================================================
Servidor FastAPI con telemetría cognitiva, autenticación Bearer,
streaming SSE estandarizado y manejo seguro de excepciones en el grafo.
"""

import os
import asyncio
import json
import time
import logging
from collections import defaultdict
from typing import AsyncGenerator, Optional

import psutil
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Security, Request
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader
from contextlib import asynccontextmanager

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langchain_core.messages import HumanMessage

from models.schemas import ChatRequest
from agent_graph import create_graph
from telemetry import (
    trace_id_var, span_id_var, parent_span_id_var,
    generate_trace_span, log_telemetry_event, start_batch_worker
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("jarvi.api")

# ---------------------------------------------------------------------------
# Seguridad – API Key
# ---------------------------------------------------------------------------
API_KEY = os.getenv("CHATBOT_MASTER_API_KEY", "sk_dev_fallback_key")
api_key_header = APIKeyHeader(name="Authorization", auto_error=False)

async def validar_api_key(auth: str | None = Security(api_key_header)):
    if not auth or auth != f"Bearer {API_KEY}":
        logger.warning("Intento de acceso no autorizado")
        raise HTTPException(status_code=403, detail="Acceso no autorizado")
    return auth

# ---------------------------------------------------------------------------
# Control de concurrencia por sesión
# ---------------------------------------------------------------------------
locks = defaultdict(asyncio.Lock)

# ---------------------------------------------------------------------------
# Taxonomía de errores
# ---------------------------------------------------------------------------
def taxonomy_error(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        return f"SWR-API-MED-{exc.status_code}"
    return "SWR-API-UNKNOWN-000"

# ---------------------------------------------------------------------------
# Ciclo de vida de la aplicación
# ---------------------------------------------------------------------------
graph = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global graph
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL no definida")
    checkpointer = AsyncPostgresSaver.from_conn_string(db_url)
    graph = create_graph(checkpointer)
    logger.info("JARVI 2.0.03 API iniciada – Grafo listo")
    start_batch_worker()
    logger.info("CTFOM worker arrancado")
    yield
    logger.info("Apagando API JARVI")

app = FastAPI(
    title="JARVI 2.0.03 API Central",
    version="2.0.03",
    lifespan=lifespan,
    dependencies=[Depends(validar_api_key)]
)

# ---------------------------------------------------------------------------
# Middleware de telemetría CTFOM
# ---------------------------------------------------------------------------
@app.middleware("http")
async def telemetry_middleware(request: Request, call_next):
    generate_trace_span()
    start = time.perf_counter()
    cpu = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory().used / (1024 * 1024)
    try:
        response = await call_next(request)
        elapsed = (time.perf_counter() - start) * 1000
        await log_telemetry_event(
            trace_id_var.get(), span_id_var.get(), "",
            layer="api", event_type="END", latency_ms=elapsed,
            cpu_percent=cpu, memory_mb=mem,
            metadata={"path": request.url.path, "method": request.method}
        )
        return response
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        await log_telemetry_event(
            trace_id_var.get(), span_id_var.get(), "",
            layer="api", event_type="ERROR", latency_ms=elapsed,
            cpu_percent=cpu, memory_mb=mem,
            error_code=taxonomy_error(e),
            metadata={"path": request.url.path}
        )
        raise

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/")
def health():
    return {"status": "ok"}

# ---------------------------------------------------------------------------
# Generador SSE con formato fijo y manejo de errores
# ---------------------------------------------------------------------------
async def generar_sse(thread_id: str, mensaje: str) -> AsyncGenerator[str, None]:
    """
    Emite eventos SSE estandarizados:
      {"type":"token","data":"..."}
      {"type":"final","response":"...","contexto_tecnico":{...}}
      {"type":"error","message":"..."}
    """
    try:
        config = {"configurable": {"thread_id": thread_id}}
        state = {
            "messages": [HumanMessage(content=mensaje)],
            "contexto_tecnico": {},
        }

        async with locks[thread_id]:
            has_tokens = False

            async for evento in graph.astream_events(state, config=config, version="v2"):
                kind = evento["event"]

                if kind == "on_chat_model_stream":
                    contenido = evento["data"]["chunk"].content
                    if contenido:
                        has_tokens = True
                        payload = {"type": "token", "data": contenido}
                        yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

                elif kind == "on_chain_end" and evento["name"] == "LangGraph":
                    estado_final = evento["data"]["output"]
                    ctx = estado_final.get("contexto_tecnico") or {}

                    # Extracción segura del último mensaje
                    messages = estado_final.get("messages")
                    respuesta_final = ""
                    if messages and isinstance(messages, list):
                        last = messages[-1]
                        if hasattr(last, "content"):
                            respuesta_final = last.content

                    payload = {
                        "type": "final",
                        "response": respuesta_final,
                        "contexto_tecnico": ctx
                    }
                    yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    break

            if not has_tokens:
                fallback = json.dumps(
                    {"type": "token", "data": "(acción ejecutada)"},
                    ensure_ascii=False
                )
                yield f"data: {fallback}\n\n"

    except Exception as e:
        logger.error(f"Error en el grafo: {e}", exc_info=True)
        error_payload = json.dumps(
            {"type": "error", "message": str(e)},
            ensure_ascii=False
        )
        yield f"data: {error_payload}\n\n"

# ---------------------------------------------------------------------------
# Endpoint /chat (streaming)
# ---------------------------------------------------------------------------
@app.post("/chat")
async def chat_endpoint(request: ChatRequest):
    return StreamingResponse(
        generar_sse(request.thread_id, request.message),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*"
        }
    )

# ---------------------------------------------------------------------------
# Endpoints auxiliares
# ---------------------------------------------------------------------------
@app.post("/ack/{trace_id}")
async def acknowledge_dispatch(trace_id: str):
    return {"status": "ACK received", "trace_id": trace_id}

@app.post("/stt")
async def speech_to_text():
    raise HTTPException(status_code=501, detail="No implementado")

@app.post("/tts")
async def text_to_speech():
    raise HTTPException(status_code=501, detail="No implementado")

@app.post("/vision/analyze")
async def analizar_factura():
    raise HTTPException(status_code=501, detail="No implementado")

@app.post("/products")
async def consultar_productos(topologia: str = "on-grid"):
    raise HTTPException(status_code=501, detail="No implementado")
