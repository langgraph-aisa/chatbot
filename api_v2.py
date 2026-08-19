"""
api_v2.py - Servidor FastAPI con trazabilidad Langfuse vía Ingestion API.
VERSIÓN 2.1.11 – Inicialización de Odoo y reseteo de flags de búsqueda.
17AGO2026.
"""
import os
import asyncio
import json
import time
import logging
import re
import uuid
from collections import defaultdict
from typing import AsyncGenerator, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime, timezone

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

import psutil
import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Depends, Security, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security import APIKeyHeader
from contextlib import asynccontextmanager, AsyncExitStack
from pydantic import BaseModel, Field

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langchain_core.messages import HumanMessage, AIMessage

# =============================================================================
# IMPORTACIONES
# =============================================================================
from observability import ObservabilityPort, LangfuseIngestionAdapter
from schemas import ChatRequest
from agent_graph import create_graph, normalizar_contacto
from telemetry import trace_id_var, span_id_var, generate_trace_span, log_telemetry_event, start_batch_worker
from db_client import get_bi_db_url, actualizar_thread
from config import settings
import audio
from supervisor_jarvi import SupervisorJarvi
from text_normalizer import limpiar_respuesta_final

# =============================================================================
# IMPORTACIONES MICDP
# =============================================================================
from project_repository import ProjectRepository
from epistemology import EpistemologyOrchestrator
import openai

# =============================================================================
# IMPORTACIÓN DEL CLIENTE ODOO DB
# =============================================================================
from odoo_db_client import odoo_db_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURACIÓN DE LANGFUSE
# =============================================================================
LANGFUSE_HOST = settings.langfuse_host
LANGFUSE_PUBLIC_KEY = settings.langfuse_public_key
LANGFUSE_SECRET_KEY = settings.langfuse_secret_key

# =============================================================================
# INICIALIZACIÓN DEL ADAPTADOR (INGESTION API)
# =============================================================================
_observability_adapter: Optional[ObservabilityPort] = None

def get_observability_adapter() -> ObservabilityPort:
    global _observability_adapter
    if _observability_adapter is None:
        try:
            _observability_adapter = LangfuseIngestionAdapter(
                public_key=LANGFUSE_PUBLIC_KEY,
                secret_key=LANGFUSE_SECRET_KEY,
                host=LANGFUSE_HOST
            )
            logger.info(f"Langfuse Ingestion adapter inicializado con host: {LANGFUSE_HOST}")
        except Exception as e:
            logger.critical(f"Error al inicializar adaptador Ingestion: {e}")
            _observability_adapter = NullObservabilityAdapter()
            logger.warning("Usando NullObservabilityAdapter (no-op)")
    return _observability_adapter

class NullObservabilityAdapter(ObservabilityPort):
    def create_trace(self, *args, **kwargs):
        logger.warning("NullObservabilityAdapter: create_trace (no-op)")
        return str(uuid.uuid4())
    def create_generation(self, *args, **kwargs):
        logger.warning("NullObservabilityAdapter: create_generation (no-op)")
    def create_score(self, *args, **kwargs):
        logger.warning("NullObservabilityAdapter: create_score (no-op)")
    def flush(self):
        pass

print("===== JARVI API v2.1.11 (CON SUPERVISOR, MICDP Y ODOO DB) =====")

# =============================================================================
# SEGURIDAD (ISO/IEC 27001)
# =============================================================================
API_KEY = settings.chatbot_master_api_key
api_key_header = APIKeyHeader(name="Authorization", auto_error=False)

async def validar_api_key(auth: str | None = Security(api_key_header)):
    if not auth or auth != f"Bearer {API_KEY}":
        raise HTTPException(status_code=403, detail="Acceso no autorizado")
    return auth

locks = defaultdict(asyncio.Lock)

def taxonomy_error(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        return f"SWR-API-MED-{exc.status_code}"
    return "SWR-API-UNKNOWN-000"

# =============================================================================
# FUNCIONES AUXILIARES PARA CÁLCULO DE SCORE (NEGOCIO)
# =============================================================================
CAMPOS_SCORE = [
    "ciudad", "empresa_electrica", "tarifa_base_gtq", "topologia",
    "calculo_carga_completado", "requiere_auditoria_electrica",
    "nombre", "whatsapp", "departamento", "municipio",
    "vendedor", "tipo_producto", "productos_interes"
]

def calcular_puntaje_completitud(ctx: dict) -> float:
    presentes = 0
    for campo in CAMPOS_SCORE:
        valor = ctx.get(campo)
        if isinstance(valor, list):
            if valor:
                presentes += 1
        elif valor:
            presentes += 1
    return round((presentes / len(CAMPOS_SCORE)) * 100, 2)

# =============================================================================
# FUNCIONES DE EXTRACCIÓN (fallback)
# =============================================================================
def extraer_whatsapp(mensaje: str) -> str | None:
    if not mensaje:
        return None
    match = re.search(r'(\+?[0-9]{1,3}[-.\s]?)?[0-9]{4,10}', mensaje)
    if match:
        numero = re.sub(r'[\s\-\.]', '', match.group(0))
        if numero.startswith('+'):
            return numero
        elif len(numero) == 8:
            return f"+502 {numero[:4]}-{numero[4:]}"
        elif len(numero) == 11 and numero.startswith('502'):
            return f"+{numero[:3]} {numero[3:7]}-{numero[7:]}"
        else:
            return f"+502 {numero}"
    return None

def extraer_nombre(mensaje: str) -> str | None:
    patrones = [
        r'(?:soy|me llamo|mi nombre es)\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)',
        r'nombre:\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)',
        r'^([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)\s*(?:de|quiere|desea)'
    ]
    for patron in patrones:
        match = re.search(patron, mensaje, re.IGNORECASE)
        if match:
            nombre = match.group(1).strip()
            if len(nombre) > 1:
                return nombre
    return None

def extraer_ubicacion(mensaje: str) -> tuple:
    from ubicacion import buscar_ubicacion
    resultado = buscar_ubicacion(mensaje)
    if resultado:
        return resultado.get("departamento"), resultado.get("municipio")
    return None, None

def extraer_consumo(mensaje: str) -> str | None:
    match = re.search(r'consumo\s*(?:de)?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:kWh|kw|kwh)', mensaje, re.IGNORECASE)
    if match:
        return f"{match.group(1)} kWh"
    return None

def extraer_empresa_electrica(mensaje: str) -> str | None:
    empresas = ["EEGSA", "DEOCSA", "DEORSA", "EEM Zacapa", "EEM Gualán", "EEM San Pedro Pinula", "EEM Jalapa",
                "EEM Puerto Barrios", "EEM Guastatoya", "EEM Sayaxché", "EEM Quetzaltenango", "EEM Retalhuleu",
                "EEM San Pedro Sacatepéquez", "EEM Huehuetenango", "EEM Joyabaj", "EEM Santa Eulalia", "EEM Tacaná",
                "Empresa Municipal Rural de Electricidad Playa Grande (Ixcán)", "EEM San Marcos"]
    for emp in empresas:
        if re.search(r'\b' + re.escape(emp) + r'\b', mensaje, re.IGNORECASE):
            return emp
    return None

def extraer_definicion_necesidad(mensaje: str) -> str | None:
    match = re.search(r'(?:necesito|quiero|deseo|estoy interesado en|busco)\s*(.+?)(?:[\.!?]|$)', mensaje, re.IGNORECASE)
    if match:
        texto = match.group(1).strip()
        palabras = texto.split()[:35]
        return " ".join(palabras)
    return None

def obtener_caso(thread_id: str) -> str:
    return thread_id.replace('-', '')[-12:] if thread_id else "000000000000"

def normalizar_whatsapp_e164(telefono: str) -> str:
    if not telefono:
        return ""
    limpio = re.sub(r'[^\d+]', '', telefono)
    if not limpio.startswith('+'):
        if limpio.startswith('502'):
            limpio = '+' + limpio
        else:
            limpio = '+502' + limpio
    return limpio

# =============================================================================
# FUNCIONES DE REDIS
# =============================================================================
REDIS_TTL = settings.redis_ttl
HISTORIAL_LIMITE = 64

def serializar_para_redis(valor):
    if isinstance(valor, (dict, list)):
        return json.dumps(valor, ensure_ascii=False)
    elif isinstance(valor, bool):
        return str(valor).lower()
    elif valor is None:
        return ""
    else:
        return str(valor)

async def guardar_sesion_redis(redis_client: Optional[redis.Redis], chat_id: str, data: dict):
    if not redis_client:
        return
    try:
        key = f"session:{chat_id}"
        data_serialized = {k: serializar_para_redis(v) for k, v in data.items()}
        await redis_client.hset(key, mapping=data_serialized)
        await redis_client.expire(key, REDIS_TTL)
        logger.info(f"Sesión guardada para chat_id {chat_id}")
    except Exception as e:
        logger.error(f"Error guardando sesión: {e}")

async def obtener_sesion_redis(redis_client: Optional[redis.Redis], chat_id: str) -> Optional[dict]:
    if not redis_client:
        return None
    try:
        key = f"session:{chat_id}"
        data = await redis_client.hgetall(key)
        if not data:
            return None
        for field in ["contexto_tecnico", "pasos_completados", "productos_interes"]:
            if field in data and isinstance(data[field], str):
                try:
                    data[field] = json.loads(data[field])
                except:
                    pass
        return data
    except Exception as e:
        logger.error(f"Error obteniendo sesión: {e}")
        return None

async def eliminar_sesion_redis(redis_client: Optional[redis.Redis], chat_id: str):
    if redis_client:
        try:
            await redis_client.delete(f"session:{chat_id}")
        except Exception as e:
            logger.error(f"Error eliminando sesión: {e}")

async def guardar_historial_redis(redis_client: Optional[redis.Redis], chat_id: str, input_msg: str, output_msg: str):
    if not redis_client:
        return
    try:
        key = f"historial:{chat_id}"
        registro = json.dumps({
            "input": input_msg,
            "output": output_msg,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        })
        await redis_client.lpush(key, registro)
        await redis_client.ltrim(key, 0, HISTORIAL_LIMITE - 1)
        logger.info(f"Historial actualizado para chat_id {chat_id}")
    except Exception as e:
        logger.error(f"Error guardando historial: {e}")

async def obtener_historial_redis(redis_client: Optional[redis.Redis], chat_id: str) -> list:
    if not redis_client:
        return []
    try:
        key = f"historial:{chat_id}"
        items = await redis_client.lrange(key, 0, -1)
        historial = []
        for item in items:
            try:
                historial.append(json.loads(item))
            except:
                continue
        return historial
    except Exception as e:
        logger.error(f"Error obteniendo historial: {e}")
        return []

# =============================================================================
# PROCESAMIENTO DE AUDIO (STT)
# =============================================================================
async def procesar_audio_url(url: str) -> str:
    if not url:
        return ""
    try:
        texto = await asyncio.to_thread(audio.transcribir_audio_desde_url, url)
        logger.info(f"Audio transcrito correctamente desde {url}")
        return texto.strip()
    except Exception as e:
        logger.error(f"Error al transcribir audio desde {url}: {e}")
        return ""

# =============================================================================
# INICIALIZACIÓN DEL SUPERVISOR
# =============================================================================
_supervisor = SupervisorJarvi(rules_path="rules.json")
logger.info("SupervisorJarvi inicializado con 45 reglas")

# =============================================================================
# INICIALIZACIÓN DEL ORQUESTADOR EPISTEMOLÓGICO (se hará en lifespan)
# =============================================================================
_epistemology = None

# =============================================================================
# DEFINICIÓN DE REPOSITORIO EN MEMORIA (FALLBACK)
# =============================================================================
class MemoryRepo:
    """Repositorio en memoria para el MICDP (fallback cuando CTFOM no está disponible)."""
    def __init__(self):
        self._data = {}
        self._states = {}
        self._answers = []
        self._incidents = []
        self._jobs = []
        self._outcomes = []
        self._vpc = {}
        self._kano = []
        self._specs = []
        self._profile = {}
        self._sessions = {}

    async def create_definition(self, thread_id, consent_hash=""):
        self._data[thread_id] = {"thread_id": thread_id, "consent_hash": consent_hash, "consent_given": True, "is_active": True}
        return True

    async def get_definition(self, thread_id):
        return self._data.get(thread_id)

    async def update_definition(self, thread_id, data):
        if thread_id in self._data:
            self._data[thread_id].update(data)

    async def get_state(self, thread_id, dimension):
        return self._states.get(f"{thread_id}_{dimension}")

    async def update_state(self, thread_id, dimension, data):
        self._states[f"{thread_id}_{dimension}"] = data

    async def add_answer(self, thread_id, question_id, answer_text, **kwargs):
        self._answers.append({"thread_id": thread_id, "question_id": question_id, "answer_text": answer_text})

    async def add_incident(self, thread_id, narrative, **kwargs):
        self._incidents.append({"thread_id": thread_id, "narrative": narrative})

    async def add_job(self, thread_id, functional_job=None, emotional_job=None, social_job=None, priority=1):
        self._jobs.append({"thread_id": thread_id, "functional_job": functional_job, "emotional_job": emotional_job, "social_job": social_job})

    async def add_outcome(self, thread_id, metric_name, current_state, desired_state, importance=1.0):
        self._outcomes.append({"thread_id": thread_id, "metric_name": metric_name})

    async def update_vpc(self, thread_id, pains=None, gains=None, jobs=None, **kwargs):
        self._vpc[thread_id] = {"pains": pains, "gains": gains, "jobs": jobs}

    async def add_kano_feature(self, thread_id, feature_name, category):
        self._kano.append({"thread_id": thread_id, "feature_name": feature_name, "category": category})

    async def add_spec(self, thread_id, spec_name, category, priority_score=0.0):
        self._specs.append({"thread_id": thread_id, "spec_name": spec_name})

    async def update_profile(self, thread_id, draft_text=None, final_text=None, **kwargs):
        self._profile[thread_id] = {"draft_text": draft_text, "final_text": final_text}

    async def get_profile(self, thread_id):
        return self._profile.get(thread_id)

    async def start_session(self, thread_id, completeness_at_start=0.0):
        self._sessions[thread_id] = {"start": time.time()}
        return 1

    async def end_session(self, session_id, **kwargs):
        pass

# =============================================================================
# PROCESAMIENTO DE PAYLOAD DE N8N (CON SUPERVISIÓN Y MICDP)
# =============================================================================
async def procesar_payload_n8n(chat_request: ChatRequest, http_request: Request) -> JSONResponse:
    """
    Procesa el payload de n8n y aplica el supervisor para validar y limpiar la respuesta.
    Además, detecta la intención de iniciar el MICDP.
    """
    # 1. Extraer datos del payload
    chat_id = chat_request.thread_id
    mensaje = chat_request.message
    nombre = chat_request.name or chat_request.metadata.get("name")
    whatsapp_raw = chat_request.phone or chat_request.metadata.get("phone")
    record = chat_request.record or chat_request.metadata.get("record", [])
    url_audio = chat_request.url_n8n_audio or chat_request.metadata.get("url_n8n_audio", "")

    # 2. Normalizar WhatsApp a E.164
    if whatsapp_raw:
        whatsapp = normalizar_whatsapp_e164(whatsapp_raw)
    else:
        whatsapp = extraer_whatsapp(mensaje) or ""

    # 3. Extraer nombre
    if not nombre:
        nombre = extraer_nombre(mensaje) or "Pendiente"

    # 4. Procesar audio
    if url_audio:
        audio_text = await procesar_audio_url(url_audio)
        if audio_text:
            mensaje = f"{mensaje}\n[Audio transcrito: {audio_text}]"

    # 5. Recuperar sesión de Redis
    sesion = await obtener_sesion_redis(redis_client, chat_id)
    historial = []
    if sesion:
        historial = await obtener_historial_redis(redis_client, chat_id)
        logger.info(f"Historial recuperado: {len(historial)} mensajes para chat_id {chat_id}")
    else:
        if record:
            historial = record
            logger.info(f"Inicializando historial desde record con {len(historial)} mensajes")
        else:
            historial = []

    # 6. Crear/actualizar sesión en Redis
    if sesion is None:
        sesion = {
            "thread_id": chat_id,
            "chat_id": chat_id,
            "whatsapp": whatsapp,
            "nombre": nombre,
            "origen": "webhook",
            "vendedor": "",
            "departamento": "",
            "municipio": "",
            "topologia": "",
            "tipo_producto": "",
            "productos_interes": [],
            "contexto_tecnico": {},
            "pasos_completados": [],
            "fase_actual": "inicio",
            "ultimo_mensaje": mensaje,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
    else:
        sesion["whatsapp"] = whatsapp
        sesion["nombre"] = nombre
        sesion["ultimo_mensaje"] = mensaje

    # 7. Inyectar nombre y whatsapp en contexto_tecnico
    contexto = sesion.get("contexto_tecnico", {})
    contexto["nombre"] = nombre
    contexto["whatsapp"] = whatsapp
    # Eliminar flag de intento de búsqueda para permitir nueva búsqueda
    contexto.pop("catalog_search_attempted", None)
    sesion["contexto_tecnico"] = contexto

    # 8. Guardar sesión en Redis
    await guardar_sesion_redis(redis_client, chat_id, sesion)

    # 9. Construir historial de mensajes
    messages = []
    for item in historial:
        if isinstance(item, dict):
            messages.append(HumanMessage(content=item.get("input", "")))
            messages.append(AIMessage(content=item.get("output", "")))
        elif isinstance(item, list) and len(item) >= 2:
            messages.append(HumanMessage(content=item[0]))
            messages.append(AIMessage(content=item[1]))

    caso = obtener_caso(chat_id)
    mensaje_con_caso = f"{mensaje} [Caso No. {caso}]"
    messages.append(HumanMessage(content=mensaje_con_caso))

    # 10. Estado inicial
    estado_inicial = {
        "messages": messages,
        "contexto_tecnico": contexto
    }

    # 11. Configuración
    config = {
        "configurable": {"thread_id": chat_id},
        "run_name": f"caso_{caso}",
        "metadata": {
            "chat_id": chat_id,
            "origen": "webhook",
            "caso": caso,
            "whatsapp": whatsapp,
        }
    }

    # 12. Ejecutar grafo
    start_time = datetime.now(timezone.utc)
    async with locks[chat_id]:
        try:
            resultado = await graph.ainvoke(estado_inicial, config=config)
        except Exception as e:
            logger.error(f"Error en ejecución del grafo para {chat_id}: {e}")
            return JSONResponse(
                status_code=500,
                content={"status": "error", "chat_id": chat_id, "error": str(e)}
            )
    end_time = datetime.now(timezone.utc)

    # 13. Extraer respuesta
    respuesta_final = ""
    ultimo_aimessage = None
    ctx = resultado.get("contexto_tecnico", {})
    for msg in reversed(resultado.get("messages", [])):
        if isinstance(msg, AIMessage):
            respuesta_final = msg.content
            ultimo_aimessage = msg
            break

    # ===== APLICAR SUPERVISIÓN A LA RESPUESTA (api_v2) =====
    # Si MICDP está activo o se está ofreciendo, saltar la supervisión para no interferir
    if ctx.get("micdp_active", False) or ctx.get("micdp_offered", False):
        logger.info("MICDP activo o ofrecido: supervisión adicional desactivada.")
    else:
        eval_data = {
            "response": respuesta_final,
            "contexto": ctx,
            "messages": messages,
            "user_message": mensaje,
            "output_type": "response",
            "price_extractor_failed": False
        }
        evaluacion = _supervisor.evaluate(eval_data)

        if evaluacion["decision"] == "rewrite":
            if evaluacion.get("modified_response"):
                respuesta_final = evaluacion["modified_response"]
                logger.info(f"Supervisor (api) reescribió respuesta: {evaluacion['rule_id']}")
        elif evaluacion["decision"] == "block":
            if "asesor" not in respuesta_final.lower():
                respuesta_final = "Disculpe, esa información específica no está disponible en este momento. ¿Prefiere que un asesor de AISA Solar le contacte para brindarle una atención personalizada? o desea iniciar el **Proceso Conversacional para la Definición de Proyectos** (responda 'Sí' para iniciar el proceso, o 'Asesor' para contacto humano)"
                ctx["derivation_offered"] = True
                ctx["micdp_offered"] = True
            logger.warning(f"Supervisor (api) bloqueó respuesta: {evaluacion['rule_id']}")
        elif evaluacion["decision"] == "force_fallback":
            ctx["escalation_mode"] = True
            if "asesor" not in respuesta_final.lower():
                respuesta_final = "Disculpe, esa información específica no está disponible en este momento. ¿Prefiere que un asesor de AISA Solar le contacte para brindarle una atención personalizada? o desea iniciar el **Proceso Conversacional para la Definición de Proyectos** (responda 'Sí' para iniciar el proceso, o 'Asesor' para contacto humano)"
                ctx["derivation_offered"] = True
                ctx["micdp_offered"] = True
            logger.info(f"Supervisor (api) activó escalación: {evaluacion['rule_id']}")
        elif evaluacion["decision"] == "force_closure":
            if evaluacion.get("modified_response"):
                respuesta_final = evaluacion["modified_response"]
                logger.info(f"Supervisor (api) forzó cierre: {evaluacion['rule_id']}")
        elif evaluacion["decision"] == "end_conversation":
            respuesta_final = evaluacion.get("modified_response", "Gracias por contactar a AISA Solar. ¡Que tenga un excelente día!")
            ctx["conversation_end"] = True
            logger.info(f"Supervisor (api) finalizó conversación: {evaluacion['rule_id']}")

    # Limpiar formato Markdown (adicional)
    respuesta_final = limpiar_respuesta_final(respuesta_final)

    # ===== ACTUALIZAR SESIÓN EN REDIS =====
    sesion["contexto_tecnico"] = ctx
    await guardar_sesion_redis(redis_client, chat_id, sesion)

    # ===== DETECTAR SI SE OFRECIÓ DERIVACIÓN Y RESPUESTA AFIRMATIVA =====
    if "asesor" in respuesta_final.lower() or "proceso conversacional" in respuesta_final.lower():
        ctx["derivation_offered"] = True
        ctx["micdp_offered"] = True
    if ctx.get("derivation_offered") and mensaje.lower() in ["sí", "si", "claro", "adelante", "iniciar", "proyecto", "me interesa"]:
        ctx["micdp_accepted"] = True
        ctx["derivation_offered"] = False
        sesion["contexto_tecnico"] = ctx
        await guardar_sesion_redis(redis_client, chat_id, sesion)

    # ===== AÑADIR CASO AL FINAL DE LA RESPUESTA =====
    if respuesta_final and not respuesta_final.endswith(f"[Caso No. {caso}]"):
        respuesta_final = f"{respuesta_final} [Caso No. {caso}]"

    # 14. Instrumentación Langfuse
    adapter = get_observability_adapter()
    trace_id_langfuse = None
    try:
        trace_id_langfuse = adapter.create_trace(
            name=f"chat_{caso}",
            user_id=whatsapp,
            session_id=chat_id,
            metadata={
                "chat_id": chat_id,
                "origen": "webhook",
                "caso": caso,
                "whatsapp": whatsapp
            },
            input_data={"message": mensaje}
        )
        logger.info(f"Traza Langfuse creada con sessionId={chat_id}")
    except Exception as e:
        logger.error(f"Error al crear traza Langfuse: {e}")

    if trace_id_langfuse and ultimo_aimessage:
        try:
            usage = ultimo_aimessage.response_metadata.get('token_usage', {})
            total_tokens = usage.get('total_tokens', 0)
            if total_tokens > 0:
                adapter.create_generation(
                    trace_id=trace_id_langfuse,
                    name=f"LLM Generation {caso}",
                    model="gpt-4o-mini",
                    input_data={"messages": mensaje},
                    output_data={"response": respuesta_final},
                    usage={
                        "input": usage.get('prompt_tokens', 0),
                        "output": usage.get('completion_tokens', 0),
                        "total": total_tokens
                    },
                    start_time=start_time,
                    end_time=end_time,
                    metadata={"chat_id": chat_id, "origen": "webhook"}
                )
                logger.info(f"Generación creada con tokens: {total_tokens}")
        except Exception as e:
            logger.error(f"Error al crear generación: {e}")

    if trace_id_langfuse:
        try:
            puntaje = calcular_puntaje_completitud(ctx)
            accion = "Calificado" if puntaje >= 60 else "No Calificado"
            adapter.create_score(
                trace_id=trace_id_langfuse,
                name="Completitud Lead",
                value=puntaje,
                data_type="NUMERIC"
            )
            adapter.create_score(
                trace_id=trace_id_langfuse,
                name="Acción Lead",
                value=accion,
                data_type="CATEGORICAL"
            )
            logger.info(f"Scores registrados para trace {trace_id_langfuse}")
        except Exception as e:
            logger.error(f"Error al crear scores: {e}")

    adapter.flush()

    # 15. Persistir historial y actualizar thread
    if respuesta_final:
        await guardar_historial_redis(redis_client, chat_id, mensaje_con_caso, respuesta_final)

        productos_raw = ctx.get("productos_interes", [])
        nombres_productos = []
        if isinstance(productos_raw, list):
            for item in productos_raw:
                if isinstance(item, dict):
                    nombres_productos.append(item.get("nombre"))
                elif isinstance(item, str):
                    nombres_productos.append(item)

        await actualizar_thread(
            thread_id=chat_id,
            nombre=nombre,
            whatsapp=whatsapp,
            productos=nombres_productos,
            vendedor=ctx.get("vendedor"),
            trace_id=trace_id_langfuse,
            cumulative_cost=0.0,
            metadata_adicional=ctx
        )

    # 16. Devolver respuesta
    return JSONResponse(
        status_code=200,
        content={
            "status": "success",
            "chat_id": chat_id,
            "response": respuesta_final,
            "langfuse_trace_id": trace_id_langfuse,
            "contexto_tecnico": ctx
        }
    )

# =============================================================================
# CICLO DE VIDA DE LA APLICACIÓN
# =============================================================================
graph = None
redis_client = None
ctfom_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global graph, redis_client, _epistemology, ctfom_pool
    print("=== INICIO DEL LIFESPAN ===")
    print("Usando Ingestion API de Langfuse (compatible con OSS)")

    get_observability_adapter()

    db_url = get_db_url()
    print("DB URL de checkpoints configurada")
    async with AsyncExitStack() as stack:
        checkpoint_pool = AsyncConnectionPool(
            conninfo=db_url,
            min_size=0,
            max_size=10,
            max_idle=300,
            kwargs={
                "autocommit": True,
                "prepare_threshold": 0,
                "row_factory": dict_row,
            },
            check=AsyncConnectionPool.check_connection,
            open=False,
        )

        checkpoint_pool = await stack.enter_async_context(checkpoint_pool)

        checkpointer = AsyncPostgresSaver(checkpoint_pool)
        await checkpointer.setup()
        graph = create_graph(checkpointer)
        print("JARVI 2.0 API inicializada – Grafo listo")

        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            redis_client = redis.from_url(redis_url, decode_responses=True)
            print("Conexión a Redis establecida")
        else:
            print("REDIS_URL no configurada – buffer de sesión deshabilitado")

        # ===== INICIALIZAR ORQUESTADOR EPISTEMOLÓGICO (con fallback en memoria) =====
        global _epistemology
        try:
            ctfom_db_url = settings.ctfom_database_url
            if ctfom_db_url:
                try:
                    parsed = urlparse(ctfom_db_url)
                    query = parse_qs(parsed.query)
                    for key in ["pool_size", "max_overflow", "pool_timeout"]:
                        query.pop(key, None)
                    clean_query = urlencode(query, doseq=True)
                    clean_url = urlunparse(parsed._replace(query=clean_query))

                    ctfom_pool = await asyncpg.create_pool(clean_url, min_size=1, max_size=5)
                    repo = ProjectRepository(ctfom_pool)
                    openai_client = openai.OpenAI(api_key=settings.openai_api_key)
                    _epistemology = EpistemologyOrchestrator(repo, openai_client)
                    print("✅ Orquestador Epistemológico inicializado correctamente con CTFOM")
                except Exception as e:
                    print(f"❌ Error al inicializar orquestador con CTFOM: {e}")
                    print("⚠️ Usando repositorio en memoria para MICDP (sin persistencia)")
                    repo = MemoryRepo()
                    openai_client = openai.OpenAI(api_key=settings.openai_api_key)
                    _epistemology = EpistemologyOrchestrator(repo, openai_client)
                    print("✅ Orquestador Epistemológico inicializado correctamente en modo memoria")
            else:
                print("⚠️ CTFOM_DATABASE_URL no configurada. Usando repositorio en memoria para MICDP.")
                repo = MemoryRepo()
                openai_client = openai.OpenAI(api_key=settings.openai_api_key)
                _epistemology = EpistemologyOrchestrator(repo, openai_client)
                print("✅ Orquestador Epistemológico inicializado correctamente en modo memoria")
        except Exception as e:
            print(f"❌ Error crítico al inicializar orquestador: {e}")
            _epistemology = None

        # ===== INICIALIZAR CLIENTE ODOO DB =====
        try:
            await odoo_db_client.connect()
            print("✅ Cliente Odoo DB inicializado correctamente.")
        except Exception as e:
            print(f"❌ Error al inicializar cliente Odoo DB: {e}")

        start_batch_worker()
        yield

    if ctfom_pool:
        await ctfom_pool.close()
    if redis_client:
        await redis_client.close()
    await odoo_db_client.close()
    print("Apagando API JARVI")

app = FastAPI(title="JARVI 2.0 API Central", version="2.1.11",
              lifespan=lifespan, dependencies=[Depends(validar_api_key)])

# =============================================================================
# MIDDLEWARE DE TELEMETRÍA
# =============================================================================
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

# =============================================================================
# HEALTH CHECK
# =============================================================================
@app.get("/health")
async def health_check():
    odoo_status = "connected" if odoo_db_client and odoo_db_client._initialized else "disconnected"
    status = {
        "service": "jarvi-backend-production",
        "version": "2.1.11",
        "redis": "connected" if redis_client else "disconnected",
        "graph": "ready" if graph else "unavailable",
        "langfuse": "Ingestion API adapter",
        "supervisor": "active",
        "micdp": "active" if _epistemology else "inactive",
        "odoo_db": odoo_status,
        "status": "ok"
    }
    return status

# =============================================================================
# ENDPOINT ÚNICO: /chat
# =============================================================================
@app.post("/chat")
async def chat_endpoint(request: ChatRequest, http_request: Request):
    return await procesar_payload_n8n(request, http_request)

# =============================================================================
# FUNCIÓN AUXILIAR PARA OBTENER DB URL
# =============================================================================
def sanear_db_url(db_url: str) -> str:
    if not db_url:
        return db_url
    try:
        parsed = urlparse(db_url)
        query = parse_qs(parsed.query)
        for p in ["pool_size", "max_overflow", "pool_timeout"]:
            query.pop(p, None)
        clean_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(query=clean_query))
    except Exception:
        return db_url

def get_db_url() -> str:
    raw = os.getenv("DATABASE_URL")
    if raw:
        return sanear_db_url(raw)
    ctfom_url = os.getenv("CTFOM_DATABASE_URL")
    if ctfom_url:
        print("DATABASE_URL no definida. Usando CTFOM_DATABASE_URL para checkpoints de LangGraph.")
        return sanear_db_url(ctfom_url)
    raise RuntimeError("No se encontró DATABASE_URL ni CTFOM_DATABASE_URL.")
