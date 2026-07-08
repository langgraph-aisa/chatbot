"""
api.py - Servidor FastAPI con checkpointing garantizado.
Se usa ainvoke para asegurar persistencia y luego se transmiten los tokens.
Se conservan todos los middlewares, autenticación, telemetría y logs.
"""

import os
import time
import logging
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import psutil
from fastapi import FastAPI, HTTPException, Depends, Security, Request
from fastapi.security import APIKeyHeader
from contextlib import asynccontextmanager, AsyncExitStack

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from agent_graph import create_graph
from routes.auxiliary import router as auxiliary_router
from routes.chat import router as chat_router
from services.chat_service import ChatService
from telemetry import (
    trace_id_var, span_id_var, parent_span_id_var,
    generate_trace_span, log_telemetry_event, start_batch_worker
)

# ---------------------------------------------------------------------------
# Configuración de logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("jarvi.api")

# ---------------------------------------------------------------------------
# Seguridad – autenticación por API Key (ISO/IEC 27001)
# ---------------------------------------------------------------------------
API_KEY = os.getenv("CHATBOT_MASTER_API_KEY", "sk_dev_fallback_key")
api_key_header = APIKeyHeader(name="Authorization", auto_error=False)

async def validar_api_key(auth: str | None = Security(api_key_header)):
    if not auth or auth != f"Bearer {API_KEY}":
        raise HTTPException(status_code=403, detail="Acceso no autorizado")
    return auth

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
        raise RuntimeError("DATABASE_URL no definida – servicio no disponible")

    # Sanitizar URL
    try:
        parsed = urlparse(db_url)
        query = parse_qs(parsed.query)
        for p in ["pool_size", "max_overflow", "pool_timeout"]:
            query.pop(p, None)
        clean_query = urlencode(query, doseq=True)
        db_url_clean = urlunparse(parsed._replace(query=clean_query))
    except Exception:
        db_url_clean = db_url

    async with AsyncExitStack() as stack:
        logger.info("Inicializando Pool de conexiones...")
        raw_checkpointer = AsyncPostgresSaver.from_conn_string(db_url_clean)
        checkpointer = await stack.enter_async_context(raw_checkpointer)
        await checkpointer.setup()
        graph = create_graph(checkpointer)
        app.state.graph = graph
        app.state.chat_service = ChatService(graph)
        logger.info("JARVI 2.0 API inicializada – Grafo listo")
        start_batch_worker()
        logger.info("CTFOM: worker de telemetría iniciado")
        yield
    logger.info("Apagando API JARVI")

app = FastAPI(title="JARVI 2.0 API Central", version="2.0.03",
              lifespan=lifespan, dependencies=[Depends(validar_api_key)])

app.include_router(chat_router)
app.include_router(auxiliary_router)

# ---------------------------------------------------------------------------
# Middleware de telemetría
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

