"""
telemetry.py
═══════════════════════════════════════════════════════════════════════
Telemetría CTFOM (infraestructura y auditoría).
Captura eventos de ejecución con metadata enriquecida.

VERSIÓN HOMOGENEIZADA 2.0.04 - Soporte para funciones síncronas y asíncronas.
Cumple con ISO/IEC 25010 (mantenibilidad) y DORA (registro de incidentes).
"""
import asyncio  # <--- NUEVA IMPORTACIÓN
import os
import json
import uuid
import logging
import functools
from contextvars import ContextVar
from typing import Optional, Any
from psycopg_pool import AsyncConnectionPool
from psycopg.conninfo import conninfo_to_dict

logger = logging.getLogger("jarvi.telemetry")

# Contexto de trazabilidad (W3C Trace Context)
trace_id_var: ContextVar[str] = ContextVar("trace_id", default="")
span_id_var: ContextVar[str] = ContextVar("span_id", default="")
parent_span_id_var: ContextVar[str] = ContextVar("parent_span_id", default="")

_event_queue: asyncio.Queue = asyncio.Queue()
_pool: Optional[AsyncConnectionPool] = None
_telemetry_loop: Optional[asyncio.AbstractEventLoop] = None
_batch_worker_task: Optional[asyncio.Task] = None

def start_batch_worker() -> asyncio.Task:
    """Inicia el worker de telemetría en el event loop activo."""
    loop = asyncio.get_running_loop()
    global _telemetry_loop, _batch_worker_task
    _telemetry_loop = loop
    if _batch_worker_task is None or _batch_worker_task.done():
        _batch_worker_task = loop.create_task(_batch_worker())
    return _batch_worker_task

def schedule_telemetry_event(*args: Any, **kwargs: Any):
    """Agenda un evento de telemetría desde código síncrono o asíncrono."""
    coro = log_telemetry_event(*args, **kwargs)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        if _telemetry_loop and _telemetry_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, _telemetry_loop)
            def _log_failure(done_future):
                try:
                    done_future.result()
                except Exception as exc:
                    logger.error("Error al registrar telemetría desde thread: %s", exc)
            future.add_done_callback(_log_failure)
            return future
        coro.close()
        logger.warning("Evento de telemetría descartado: no hay event loop activo")
        return None
    return loop.create_task(coro)

async def _get_pool():
    """Obtiene o crea un pool de conexiones a PostgreSQL (CTFOM)."""
    global _pool

    if _pool is None:
        db_url = os.getenv("CTFOM_DATABASE_URL")

        if not db_url:
            raise ValueError("CTFOM_DATABASE_URL no configurada")

        params = conninfo_to_dict(db_url)

        # Eliminar parámetros que pertenecen al pool y no a PostgreSQL
        for key in ("pool_size", "max_overflow", "pool_timeout"):
            params.pop(key, None)

        pool = AsyncConnectionPool(
            conninfo="",
            kwargs=params,
            min_size=0,
            max_size=10,
            max_idle=300,
            check=AsyncConnectionPool.check_connection,
            open=False,
        )

        try:
            await pool.open()
            await pool.wait()
        except Exception:
            await pool.close()
            raise

        _pool = pool

    return _pool

async def _batch_worker():
    """Consume eventos de la cola y los inserta en lote en telemetry_events."""
    while True:
        batch = []
        batch.append(await _event_queue.get())
        try:
            while len(batch) < 100:
                batch.append(_event_queue.get_nowait())
        except asyncio.QueueEmpty:
            pass
        pool = await _get_pool()
        async with pool.connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            """INSERT INTO telemetry_events 
                            (trace_id, span_id, parent_span_id, thread_id, run_id, layer, node_name,
                             event_type, latency_ms, severity, error_code, cpu_percent, memory_mb,
                             dispatch_success, metadata)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            batch
                        )
            except Exception as e:
                logger.error(f"Error en batch insert (Auditoría Forense): {e}")
        for _ in range(len(batch)):
            _event_queue.task_done()

async def log_telemetry_event(trace_id: str, span_id: str, parent_span_id: str,
                              layer: str, event_type: str, node_name: str = None,
                              latency_ms: int = None, severity: str = 'INFO',
                              error_code: str = None, cpu_percent: float = None,
                              memory_mb: float = None, dispatch_success: bool = None,
                              metadata: dict = None,
                              thread_id: str = None, run_id: str = None):
    """
    Agrega un evento a la cola de telemetría.
    El campo metadata se enriquece con datos de negocio desde api.py (hook pre-ejecución).
    """
    if not hasattr(log_telemetry_event, "_started"):
        start_batch_worker()
        log_telemetry_event._started = True

    event = (
        trace_id or str(uuid.uuid4()),
        span_id or str(uuid.uuid4()),
        parent_span_id or None,
        thread_id,
        run_id,
        layer,
        node_name,
        event_type,
        latency_ms,
        severity,
        error_code,
        cpu_percent,
        memory_mb,
        dispatch_success,
        json.dumps(metadata or {}),
    )
    await _event_queue.put(event)

def generate_trace_span():
    """Genera nuevos IDs de traza y span (W3C Trace Context)."""
    trace_id = str(uuid.uuid4())
    span_id = str(uuid.uuid4())
    trace_id_var.set(trace_id)
    span_id_var.set(span_id)
    parent_span_id_var.set("")
    return trace_id, span_id

# =============================================================================
# DECORADOR @observe_node HOMOGENEIZADO
# =============================================================================
def observe_node(layer: str = "graph", node_name: str = ""):
    """
    Decorador homogeneizado para CTFOM. Detecta si la función es síncrona o asíncrona
    y la envuelve correctamente para registrar eventos de telemetría.
    """
    def decorator(func):
        # Caso 1: Función Asíncrona
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                trace_id = trace_id_var.get()
                span_id = str(uuid.uuid4())
                parent = span_id_var.get()
                span_id_var.set(span_id)
                start = asyncio.get_event_loop().time()
                try:
                    result = await func(*args, **kwargs)
                    elapsed = (asyncio.get_event_loop().time() - start) * 1000
                    schedule_telemetry_event(
                        trace_id, span_id, parent,
                        layer=layer, node_name=node_name,
                        event_type="END", latency_ms=elapsed
                    )
                    return result
                except Exception as e:
                    elapsed = (asyncio.get_event_loop().time() - start) * 1000
                    schedule_telemetry_event(
                        trace_id, span_id, parent,
                        layer=layer, node_name=node_name,
                        event_type="ERROR", latency_ms=elapsed,
                        error_code=f"SWR-LGG-{type(e).__name__}"
                    )
                    raise
                finally:
                    span_id_var.set(parent)
            return async_wrapper
        
        # Caso 2: Función Síncrona
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                trace_id = trace_id_var.get()
                span_id = str(uuid.uuid4())
                parent = span_id_var.get()
                span_id_var.set(span_id)
                start = asyncio.get_event_loop().time()  # También funciona en hilos
                try:
                    result = func(*args, **kwargs)
                    elapsed = (asyncio.get_event_loop().time() - start) * 1000
                    schedule_telemetry_event(
                        trace_id, span_id, parent,
                        layer=layer, node_name=node_name,
                        event_type="END", latency_ms=elapsed
                    )
                    return result
                except Exception as e:
                    elapsed = (asyncio.get_event_loop().time() - start) * 1000
                    schedule_telemetry_event(
                        trace_id, span_id, parent,
                        layer=layer, node_name=node_name,
                        event_type="ERROR", latency_ms=elapsed,
                        error_code=f"SWR-LGG-{type(e).__name__}"
                    )
                    raise
                finally:
                    span_id_var.set(parent)
            return sync_wrapper
    return decorator
