"""
agent_graph.py - Grafo agéntico de JARVI 2.0 con instrumentación CTFOM.
VERSIÓN 2.7.9 – Búsqueda flexible con difflib y selección de opciones (corregida).
17AGO2026.
"""
import os
import time
import uuid
import asyncio
import requests
import re
import json
import difflib
import functools
import logging
from typing import Annotated, TypedDict, Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timezone

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.base import BaseCheckpointSaver

from utils.sanitize import sanitize_pii
import config
from audit import auditar_fase
from ontology import (
    obtener_fragmento_ontologia,
    cargar_ontologia,
    obtener_productos_relevantes,
    inferir_tag_por_mensaje,
    get_requirements_by_tag,
    get_requires_diagnostic,
    get_dimensionamiento_by_tag,
    get_precio_by_tag
)
from telemetry import trace_id_var, span_id_var, schedule_telemetry_event
from ubicacion import buscar_ubicacion
from prompt_manager import get_prompt
from supervisor_jarvi import SupervisorJarvi

# =============================================================================
# IMPORTACIÓN DE MÓDULOS MICDP
# =============================================================================
from project_repository import ProjectRepository
from epistemology import EpistemologyOrchestrator
import openai
import asyncpg
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# =============================================================================
# IMPORTACIÓN DEL CLIENTE ODOO Y PRICE EXTRACTOR
# =============================================================================
from odoo_db_client import odoo_db_client
from price_extractor import extract_price_from_url

logger = logging.getLogger(__name__)
from config import settings

# =============================================================================
# INSTANCIAS GLOBALES
# =============================================================================
_supervisor = SupervisorJarvi(rules_path="rules.json")
logger.info("SupervisorJarvi inicializado con 45 reglas")

_epistemology = None
_epistemology_lock = asyncio.Lock()

# =============================================================================
# FUNCIÓN PARA NORMALIZAR CONSULTA DEL USUARIO (fuera de create_graph)
# =============================================================================
def normalizar_consulta(texto: str) -> str:
    """
    Limpia y extrae términos clave de la consulta del usuario.
    Elimina palabras vacías, conserva números, unidades y palabras técnicas.
    """
    if not texto:
        return ""
    texto = re.sub(r'\[caso no\..*?\]', '', texto, flags=re.IGNORECASE)
    stopwords = {'por', 'favor', 'dame', 'precio', 'información', 'info', 
                 'solo', 'solamente', 'necesito', 'quiero', 'consultar',
                 'producto', 'batería', 'bateria', 'gel', 'ah', 'v', 'volt',
                 'de', 'la', 'el', 'los', 'las', 'para', 'con', 'sin', 'y', 'o',
                 'porfavor', 'dademe', 'soloe', 'vaor'}
    tokens = re.findall(r'[a-záéíóúñ]+|\d+\.?\d*', texto.lower())
    tokens_filtrados = [t for t in tokens if t not in stopwords and len(t) > 1]
    return " ".join(tokens_filtrados)

# =============================================================================
# CARGA DEL MARCO ACADÉMICO (REFERENCIAS APA)
# =============================================================================
_ACADEMIC_FRAMEWORK = None

def get_academic_framework() -> str:
    """Retorna el marco ontológico, epistemológico y fenomenológico con referencias APA 8ª."""
    global _ACADEMIC_FRAMEWORK
    if _ACADEMIC_FRAMEWORK is not None:
        return _ACADEMIC_FRAMEWORK

    json_path = os.path.join(os.path.dirname(__file__), "academic_framework.json")
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"No se pudo cargar academic_framework.json: {e}")
        return "Lo siento, no puedo acceder a la base académica en este momento. Por favor, intente más tarde."

    lines = []
    lines.append("📘 **BASE ACADÉMICA Y METODOLÓGICA DE JARVI – MICDP**")
    lines.append("")
    lines.append("**🔍 Marco Ontológico:**")
    lines.append(data["framework"]["ontological"])
    lines.append("")
    lines.append("**🧠 Marco Epistemológico:**")
    lines.append(data["framework"]["epistemological"])
    lines.append("")
    lines.append("**💬 Marco Fenomenológico:**")
    lines.append(data["framework"]["phenomenological"])
    lines.append("")
    lines.append("**📋 Metodología:**")
    lines.append(data["methodology"])
    lines.append("")
    lines.append("**📚 Referencias Bibliográficas (APA 8ª edición):**")
    for i, ref in enumerate(data["references"], 1):
        lines.append(f"{i}. {ref['authors']} ({ref['year']}). *{ref['title']}*. {ref['source']}.")
    lines.append("")
    lines.append("Estos fundamentos garantizan el rigor científico y la trazabilidad de cada interacción.")
    _ACADEMIC_FRAMEWORK = "\n".join(lines)
    return _ACADEMIC_FRAMEWORK

# =============================================================================
# CONFIGURACIÓN DE API KEY
# =============================================================================
def get_llm():
    return ChatOpenAI(
        openai_api_key=settings.openai_api_key,
        model="gpt-4o-mini",
        temperature=0.1,
        timeout=60.0,
        max_retries=5,
        default_headers={"User-Agent": "JARVI/2.7.9"}
    )

# =============================================================================
# CÓDIGOS DE ÁREA
# =============================================================================
CODIGOS_AREA = {
    "belice": "+501", "costa rica": "+506", "el salvador": "+503",
    "guatemala": "+502", "honduras": "+504", "nicaragua": "+505",
    "panama": "+507", "panamá": "+507"
}

def normalizar_contacto(nombre_raw: str, whatsapp_raw: str, ubicacion_raw: str) -> tuple:
    nombre_str = str(nombre_raw).strip() if nombre_raw else "Usuario"
    nombre_partes = nombre_str.split()
    nombre_normalizado = " ".join([p.capitalize() for p in nombre_partes]) if nombre_partes else "Usuario"

    codigo_area = "+502"
    ubicacion_lower = str(ubicacion_raw).lower() if ubicacion_raw else ""
    for pais, codigo in CODIGOS_AREA.items():
        if pais in ubicacion_lower:
            codigo_area = codigo
            break

    digits = re.sub(r'\D', '', whatsapp_raw if whatsapp_raw else "")
    if not digits:
        whatsapp_formateado = "Pendiente"
    else:
        codigo_limpio = codigo_area.replace('+', '')
        if digits.startswith(codigo_limpio) and len(digits) >= len(codigo_limpio) + 8:
            base = digits[len(codigo_limpio):]
        else:
            base = digits
        if len(base) >= 8:
            whatsapp_formateado = f"{codigo_area} {base[:4]}-{base[4:]}"
        else:
            whatsapp_formateado = f"{codigo_area} {base}"
    return nombre_normalizado, whatsapp_formateado

# =============================================================================
# ESQUEMAS
# =============================================================================
class ExtractorContacto(BaseModel):
    nombre: Optional[str] = Field(None, description="Nombre de pila y apellidos.")
    telefono: Optional[str] = Field(None, description="Número telefónico.")

class ChecklistExtract(BaseModel):
    nombre: Optional[str] = Field(None, description="Nombre completo del cliente.")
    whatsapp: Optional[str] = Field(None, description="Número de teléfono en formato E.164.")
    departamento: Optional[str] = Field(None, description="Departamento de Guatemala.")
    municipio: Optional[str] = Field(None, description="Municipio de Guatemala.")
    ciudad: Optional[str] = Field(None, description="Ciudad o localidad.")
    empresa_electrica: Optional[str] = Field(None, description="Empresa distribuidora de electricidad (EEGSA, DEOCSA, etc.).")
    tarifa_base_gtq: Optional[float] = Field(None, description="Tarifa eléctrica en GTQ por kWh.")
    topologia: Optional[str] = Field(None, description="On-Grid, Off-Grid, o No aplica.")
    calculo_carga_completado: Optional[bool] = Field(None, description="Si ya se calculó la carga eléctrica.")
    requiere_auditoria_electrica: Optional[bool] = Field(None, description="Si el producto requiere diagnóstico eléctrico.")
    vendedor: Optional[str] = Field(None, description="Nombre del vendedor asignado.")
    tipo_producto: Optional[str] = Field(None, description="sistema o unitario.")
    productos_interes: Optional[List[str]] = Field(None, description="Lista de nombres de productos de interés.")

class InferenciaEnergetica(TypedDict):
    ciudad: Optional[str]
    empresa_electrica: Optional[str]
    tarifa_base_gtq: Optional[float]
    topologia: Optional[str]
    calculo_carga_completado: bool
    requiere_auditoria_electrica: bool
    nombre: Optional[str]
    whatsapp: Optional[str]
    departamento: Optional[str]
    municipio: Optional[str]
    vendedor: Optional[str]
    tipo_producto: Optional[str]
    productos_interes: Optional[list]
    product_tag: Optional[str]
    requisitos: Optional[List[Dict]]
    checklist_universal: Optional[Dict]
    fecha_estimada_compra: Optional[str]
    score_actual: Optional[float]
    cierre_realizado: Optional[bool]
    iteraciones_sin_cambio: Optional[int]
    escalation_mode: Optional[bool]
    authorization_asked: Optional[bool]
    conversation_end: Optional[bool]
    derivation_offered: Optional[bool]
    catalog_search_mode: Optional[bool]
    awaiting_expansion: Optional[bool]
    current_product_id: Optional[int]
    odoo_search_results: Optional[List[Dict]]
    fuente_producto: Optional[str]
    micdp_accepted: Optional[bool]
    micdp_offered: Optional[bool]
    micdp_active: Optional[bool]
    catalog_search_attempted: Optional[bool]
    esperando_seleccion: Optional[bool]
    productos_opciones: Optional[List[tuple]]

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    contexto_tecnico: InferenciaEnergetica

# =============================================================================
# FUNCIONES AUXILIARES PARA CHECKLIST Y SCORING
# =============================================================================
CAMPOS_SCORE_UNIVERSAL = [
    "nombre", "whatsapp", "departamento", "municipio", "ciudad",
    "empresa_electrica", "tarifa_base_gtq", "topologia",
    "calculo_carga_completado", "requiere_auditoria_electrica",
    "vendedor", "tipo_producto", "productos_interes"
]

def inicializar_checklist_universal(ctx: dict) -> dict:
    checklist = {}
    for campo in CAMPOS_SCORE_UNIVERSAL:
        valor = ctx.get(campo)
        if campo == "productos_interes" and isinstance(valor, list) and valor:
            checklist[campo] = "completado"
        elif campo == "tipo_producto" and valor:
            checklist[campo] = "completado"
        elif campo == "vendedor" and valor:
            checklist[campo] = "completado"
        elif campo == "calculo_carga_completado" and valor:
            checklist[campo] = "completado"
        elif campo == "requiere_auditoria_electrica" and valor is not None:
            checklist[campo] = "completado"
        elif isinstance(valor, str) and valor and valor != "Pendiente":
            checklist[campo] = "completado"
        elif isinstance(valor, float) and valor > 0:
            checklist[campo] = "completado"
        else:
            checklist[campo] = "pendiente"
    return checklist

def calcular_puntaje_completitud(ctx: dict) -> float:
    checklist = ctx.get("checklist_universal")
    if not checklist:
        checklist = inicializar_checklist_universal(ctx)
        ctx["checklist_universal"] = checklist
    completados = sum(1 for status in checklist.values() if status == "completado")
    return round((completados / len(CAMPOS_SCORE_UNIVERSAL)) * 100, 2)

def normalizar_productos_interes(productos_raw) -> List[Dict[str, str]]:
    if not productos_raw:
        return []
    if isinstance(productos_raw, list):
        if productos_raw and isinstance(productos_raw[0], str):
            ontologia = cargar_ontologia()
            resultado = []
            for nombre in productos_raw:
                if not nombre:
                    continue
                tag = None
                for key, item in ontologia.items():
                    if isinstance(item, dict) and item.get("nombre", "").lower() == nombre.lower():
                        tag = key
                        break
                resultado.append({"nombre": nombre, "tag": tag or "desconocido"})
            return resultado
        elif productos_raw and isinstance(productos_raw[0], dict):
            return productos_raw
    return []

# =============================================================================
# DECORADOR CTFOM
# =============================================================================
def observe_node(layer: str = "graph", node_name: str = ""):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            trace_id = trace_id_var.get()
            span_id = str(uuid.uuid4())
            parent = span_id_var.get()
            span_id_var.set(span_id)
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                elapsed = (time.perf_counter() - start) * 1000
                schedule_telemetry_event(
                    trace_id, span_id, parent,
                    layer=layer, node_name=node_name,
                    event_type="END", latency_ms=elapsed
                )
                return result
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                schedule_telemetry_event(
                    trace_id, span_id, parent,
                    layer=layer, node_name=node_name,
                    event_type="ERROR", latency_ms=elapsed,
                    error_code=f"SWR-LGG-{type(e).__name__}"
                )
                raise
            finally:
                span_id_var.set(parent)
        return wrapper
    return decorator

# =============================================================================
# HERRAMIENTA DE ENVÍO A N8N
# =============================================================================
@tool
async def procesar_oportunidad_backend(
    nombre_apellidos: str,
    departamento_municipio: str,
    consumo_actual: str,
    empresa_electrica: str,
    definicion_necesidad: str,
    listado_equipos_html: str,
    numero_whatsapp: str,
    resumen_18_palabras: str
) -> str:
    """
    Envía los datos del proyecto al backend de oportunidades (N8N) para su gestión comercial.
    """
    start_time = time.perf_counter()
    try:
        nombre_norm, whatsapp_norm = normalizar_contacto(nombre_apellidos, numero_whatsapp, departamento_municipio)
        endpoint = os.getenv("N8N_WEBHOOK_URL", "")
        if not endpoint:
            logger.warning("N8N_WEBHOOK_URL no configurado. Lead no enviado.")
            return "No se pudo enviar el lead: webhook no configurado."
        num_limpio = ''.join(filter(str.isdigit, whatsapp_norm))
        payload = {
            "nombre": nombre_norm,
            "whatsapp": num_limpio,
            "ubicacion": departamento_municipio,
            "consumo": consumo_actual,
            "empresa_electrica": empresa_electrica,
            "necesidad": definicion_necesidad,
            "equipos": listado_equipos_html,
            "resumen": resumen_18_palabras
        }
        await asyncio.to_thread(requests.post, endpoint, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
        logger.info(f"Lead enviado exitosamente a N8N para {whatsapp_norm}")
        schedule_telemetry_event(
            trace_id_var.get(), span_id_var.get(), "",
            layer="tool", node_name="procesar_oportunidad_backend",
            event_type="END", latency_ms=(time.perf_counter() - start_time) * 1000
        )
        return f"Lead enviado a N8N. Contacto: {whatsapp_norm}."
    except Exception as e:
        logger.error(f"Fallo en envío a N8N: {e}")
        schedule_telemetry_event(
            trace_id_var.get(), span_id_var.get(), "",
            layer="tool", node_name="procesar_oportunidad_backend",
            event_type="ERROR", latency_ms=(time.perf_counter() - start_time) * 1000,
            error_code=f"SWR-N8N-{type(e).__name__}"
        )
        return f"Error al enviar lead: {str(e)}"

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================
def extraer_intencion_humana(messages: list) -> str:
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            if isinstance(msg.content, str):
                return msg.content.lower()
            if isinstance(msg.content, list):
                return " ".join([
                    str(b.get("text", "")).lower()
                    for b in msg.content
                    if isinstance(b, dict) and "text" in b
                ])
    return ""

def extraer_tipo_producto(mensaje: str) -> Optional[str]:
    if not mensaje:
        return None
    mensaje_lower = mensaje.lower()
    patron_sistema = r'\b(sistema|kit|completo|llave en mano|instalación completa|pack solar|planta solar|proyecto llave en mano|integral)\b'
    patron_unitario = r'\b(producto|unitario|componente|inversor|panel|módulo|batería|acumulador|calentador|termo|bomba|controlador|cargador|estructura|soporte|cable|conector|regulador)\b'
    if re.search(patron_sistema, mensaje_lower):
        return "sistema"
    if re.search(patron_unitario, mensaje_lower):
        return "unitario"
    if re.search(r'\b(solar|fotovoltaico|fotovoltaica|energía solar)\b', mensaje_lower):
        return "unitario"
    return None

# =============================================================================
# FUNCIÓN PARA INICIALIZAR ORQUESTADOR DE FORMA LAZY (POR WORKER)
# =============================================================================
async def get_epistemology():
    """Obtiene o inicializa el orquestador de forma lazy (por worker)."""
    global _epistemology
    if _epistemology is not None:
        return _epistemology
    async with _epistemology_lock:
        if _epistemology is not None:
            return _epistemology
        try:
            ctfom_db_url = settings.ctfom_database_url
            if ctfom_db_url:
                parsed = urlparse(ctfom_db_url)
                query = parse_qs(parsed.query)
                for key in ["pool_size", "max_overflow", "pool_timeout"]:
                    query.pop(key, None)
                clean_query = urlencode(query, doseq=True)
                clean_url = urlunparse(parsed._replace(query=clean_query))
                pool = await asyncpg.create_pool(clean_url, min_size=1, max_size=5)
                repo = ProjectRepository(pool)
                openai_client = openai.OpenAI(api_key=settings.openai_api_key)
                _epistemology = EpistemologyOrchestrator(repo, openai_client)
                logger.info("Orquestador inicializado lazy con CTFOM")
            else:
                from api_v2 import MemoryRepo
                repo = MemoryRepo()
                openai_client = openai.OpenAI(api_key=settings.openai_api_key)
                _epistemology = EpistemologyOrchestrator(repo, openai_client)
                logger.info("Orquestador inicializado lazy en memoria")
        except Exception as e:
            logger.error(f"Error en inicialización lazy del orquestador: {e}")
            from api_v2 import MemoryRepo
            repo = MemoryRepo()
            openai_client = openai.OpenAI(api_key=settings.openai_api_key)
            _epistemology = EpistemologyOrchestrator(repo, openai_client)
            logger.warning("Orquestador inicializado lazy en memoria (fallback)")
        return _epistemology

# =============================================================================
# CONSTRUCCIÓN DEL GRAFO (dentro de esta función se definen los nodos)
# =============================================================================
def create_graph(checkpointer: BaseCheckpointSaver):
    graph_builder = StateGraph(AgentState)
    llm = get_llm().bind_tools([procesar_oportunidad_backend])
    extractor_llm = llm.with_structured_output(ExtractorContacto)
    checklist_llm = llm.with_structured_output(ChecklistExtract)

    # -------------------------------------------------------------------------
    # NODO 1: CLASIFICADOR DE INTENCIÓN
    # -------------------------------------------------------------------------
    @auditar_fase(nombre_fase="Clasificador de Intención Comercial", criticidad="MEDIA")
    @observe_node(node_name="clasificar_intencion_comercial")
    async def clasificar_intencion_comercial_node(state: AgentState):
        ctx = dict(state.get("contexto_tecnico") or {})
        ultimo = extraer_intencion_humana(state.get("messages", []))

        messages = state.get("messages", [])
        if messages and isinstance(messages[-1], HumanMessage):
            ctx["catalog_search_attempted"] = False
            ctx["catalog_search_mode"] = False
            ctx["awaiting_expansion"] = False
            ctx["esperando_seleccion"] = False
            ctx["productos_opciones"] = []

        if not ultimo:
            return {"contexto_tecnico": ctx}
        if ctx.get("topologia"):
            return {"contexto_tecnico": ctx}
        if re.search(r'\b(on\s*grid|conectado a la red|atado a la red|sistema de red)\b', ultimo, re.IGNORECASE):
            ctx["topologia"] = "On-Grid (Sistemas Atados a la Red)"
        elif re.search(r'\b(off\s*grid|aislado|sin red|autónomo|independiente)\b', ultimo, re.IGNORECASE):
            ctx["topologia"] = "Off-Grid (Sistemas Aislados)"
        return {"contexto_tecnico": ctx}

    # -------------------------------------------------------------------------
    # NODO 2: VALIDADOR DE UBICACIÓN
    # -------------------------------------------------------------------------
    @auditar_fase(nombre_fase="Validador de Ubicación del Cliente", criticidad="MEDIA")
    @observe_node(node_name="validar_ubicacion_cliente")
    async def validar_ubicacion_cliente_node(state: AgentState):
        ctx = dict(state.get("contexto_tecnico") or {})
        ultimo = extraer_intencion_humana(state.get("messages", []))
        if not ultimo:
            return {"contexto_tecnico": ctx}
        if not ctx.get("departamento") or not ctx.get("municipio"):
            resultado = buscar_ubicacion(ultimo)
            if resultado:
                ctx["departamento"] = resultado["departamento"]
                ctx["municipio"] = resultado["municipio"]
                ctx["ciudad"] = resultado["municipio"]
                logger.info(f"Ubicación detectada: {resultado['label']}")
        if ctx.get("requiere_auditoria_electrica") and ctx.get("departamento"):
            if ctx["departamento"].lower() == "guatemala":
                ctx["empresa_electrica"] = "EEGSA"
                ctx["tarifa_base_gtq"] = 1.45
        return {"contexto_tecnico": ctx}

    # -------------------------------------------------------------------------
    # NODO 3: SELECCIÓN DE PRODUCTOS (CON FUZZY MATCHING Y OPCIONES)
    # -------------------------------------------------------------------------
    @auditar_fase(nombre_fase="Selección de Productos", criticidad="ALTA")
    @observe_node(node_name="seleccionar_productos")
    async def seleccionar_productos_node(state: AgentState):
        ctx = dict(state.get("contexto_tecnico") or {})
        ultimo = extraer_intencion_humana(state.get("messages", []))

        if ctx.get("product_tag"):
            return {"contexto_tecnico": ctx}

        logger.info("=" * 80)
        logger.info("[SELECTOR] 🚀 INICIANDO PROCESO DE SELECCIÓN DE PRODUCTOS")
        logger.info(f"[SELECTOR] 📝 Consulta del usuario: '{ultimo}'")
        logger.info("=" * 80)

        # ================================================================
        # NIVEL 1: Odoo Database (búsqueda flexible)
        # ================================================================
        logger.info("[SELECTOR] 📊 NIVEL 1: Intentando obtener producto desde Odoo DB (búsqueda flexible)")
        odoo_results = []
        try:
            consulta_limpia = normalizar_consulta(ultimo)
            logger.info(f"[SELECTOR] 📊 Consulta normalizada: '{consulta_limpia}'")
            if not consulta_limpia:
                consulta_limpia = ultimo
            odoo_results = await odoo_db_client.search_products_flexible(consulta_limpia, limit=20)
            if odoo_results:
                logger.info(f"[SELECTOR] 📊 Odoo DB - {len(odoo_results)} candidatos encontrados")
                textos_candidatos = []
                for prod in odoo_results:
                    texto = prod.get('name', '')
                    if prod.get('description_sale'):
                        texto += " " + prod['description_sale']
                    textos_candidatos.append(texto)

                puntajes = []
                for idx, texto in enumerate(textos_candidatos):
                    score = difflib.SequenceMatcher(None, consulta_limpia, texto).ratio() * 100
                    puntajes.append((idx, score, odoo_results[idx]))
                puntajes.sort(key=lambda x: x[1], reverse=True)
                mejores = puntajes[:5]

                for idx, score, prod in mejores:
                    logger.info(f"[SELECTOR] 📊   {prod.get('name')} - Score: {score:.1f}%")

                if mejores and mejores[0][1] >= 85 and (len(mejores) == 1 or (mejores[0][1] - mejores[1][1]) > 15):
                    producto_elegido = mejores[0][2]
                    ctx["product_tag"] = f"odoo_{producto_elegido['id']}"
                    ctx["fuente_producto"] = "odoo"
                    ctx["producto_odoo"] = producto_elegido
                    ctx["odoo_search_results"] = [producto_elegido]
                    ctx["tipo_producto"] = "unitario"
                    ctx["requiere_auditoria_electrica"] = False
                    ctx["requisitos"] = []
                    ctx["precio_extraido"] = odoo_db_client.format_price(producto_elegido.get('list_price'))
                    ctx["fuente_precio"] = "odoo_db"
                    ctx["fuente_detalle"] = {
                        "nivel": 1,
                        "fuente": "odoo_db",
                        "producto_id": producto_elegido.get('id'),
                        "score": mejores[0][1],
                        "precio_raw": producto_elegido.get('list_price')
                    }
                    if not ctx.get("checklist_universal"):
                        ctx["checklist_universal"] = inicializar_checklist_universal(ctx)
                    checklist = ctx["checklist_universal"]
                    checklist["productos_interes"] = "completado"
                    ctx["checklist_universal"] = checklist

                    respuesta = f"✅ Encontramos el producto: **{producto_elegido['name']}** - Precio: {ctx['precio_extraido']}"
                    logger.info(f"[SELECTOR] 📊 ✅ Coincidencia exacta: {respuesta}")
                    logger.info("=" * 80)
                    return {"messages": [AIMessage(content=respuesta)], "contexto_tecnico": ctx}
                else:
                    opciones = []
                    for i, (idx, score, prod) in enumerate(mejores, 1):
                        precio = odoo_db_client.format_price(prod.get('list_price'))
                        opciones.append(f"{i}. **{prod['name']}** - {precio}")
                    respuesta = "🔍 Encontramos varios productos similares:\n\n" + "\n".join(opciones) + "\n\n¿Cuál de estos le interesa? (Indique el número)"
                    ctx["productos_opciones"] = mejores
                    ctx["esperando_seleccion"] = True
                    logger.info("[SELECTOR] 📊 📋 Presentando opciones al usuario")
                    logger.info("=" * 80)
                    return {"messages": [AIMessage(content=respuesta)], "contexto_tecnico": ctx}
            else:
                logger.warning("[SELECTOR] 📊 Odoo DB - ⚠️ No se encontraron productos coincidentes")
                ctx["error_odoo_db"] = "No se encontraron productos coincidentes en Odoo DB"
        except Exception as e:
            logger.error(f"[SELECTOR] 📊 Odoo DB - ❌ Error inesperado: {e}")
            ctx["error_odoo_db"] = str(e)

        # ================================================================
        # NIVEL 2: Web Scraping via Ontología + Price Extractor
        # ================================================================
        logger.info("[SELECTOR] 🌐 NIVEL 2: Intentando obtener producto desde Web Scraping (AISA Solar)")
        try:
            ontologia = cargar_ontologia()
            tag = inferir_tag_por_mensaje(ultimo)
            if tag and tag in ontologia:
                item = ontologia[tag]
                if "url" in item and item["url"]:
                    logger.info(f"[SELECTOR] 🌐 Extrayendo precio desde: {item['url']}")
                    precio_data = extract_price_from_url(item["url"])
                    if precio_data:
                        if precio_data["moneda"] == "USD":
                            precio_gtq = precio_data["precio"] * 7.8
                            simbolo = "Q"
                        else:
                            precio_gtq = precio_data["precio"]
                            simbolo = precio_data.get("simbolo", "Q")
                        ctx["product_tag"] = tag
                        ctx["fuente_producto"] = "web_scraping"
                        ctx["producto_web"] = item
                        ctx["tipo_producto"] = item.get("tipo", "unitario")
                        ctx["requisitos"] = item.get("requirements", [])
                        ctx["requiere_auditoria_electrica"] = item.get("requiere_diagnostico_electrico", False)
                        ctx["precio_extraido"] = f"{simbolo} {precio_gtq:,.2f}".replace(",", ".")
                        ctx["moneda_extraida"] = "GTQ"
                        ctx["fuente_precio"] = f"web_scraping:{item['url']}"
                        ctx["fuente_detalle"] = {
                            "nivel": 2,
                            "fuente": "web_scraping",
                            "url": item["url"],
                            "precio_raw": precio_data["precio"],
                            "moneda_raw": precio_data["moneda"],
                            "selector": precio_data.get("selector"),
                            "precio_convertido": precio_gtq
                        }
                        if not ctx.get("checklist_universal"):
                            ctx["checklist_universal"] = inicializar_checklist_universal(ctx)
                        checklist = ctx["checklist_universal"]
                        checklist["productos_interes"] = "completado"
                        ctx["checklist_universal"] = checklist
                        logger.info("[SELECTOR] 🌐 ✅ Información guardada en contexto (fuente: Web Scraping)")
                        logger.info("=" * 80)
                        return {"contexto_tecnico": ctx}
                    else:
                        ctx["error_web_scraping"] = f"No se pudo extraer precio de {item['url']}"
                else:
                    ctx["error_web_scraping"] = "Producto sin URL en ontología"
            else:
                ctx["error_web_scraping"] = "Producto no encontrado en ontología"
        except Exception as e:
            logger.error(f"[SELECTOR] 🌐 ❌ Error en Web Scraping: {e}")
            ctx["error_web_scraping"] = str(e)

        # ================================================================
        # NIVEL 3: Ontología Local (sin precio)
        # ================================================================
        logger.info("[SELECTOR] 📋 NIVEL 3: Intentando obtener producto desde Ontología Local")
        try:
            tag = inferir_tag_por_mensaje(ultimo)
            if tag and tag in ontologia:
                item = ontologia[tag]
                ctx["product_tag"] = tag
                ctx["fuente_producto"] = "ontologia"
                ctx["producto_ontologia"] = item
                ctx["tipo_producto"] = item.get("tipo", "unitario")
                ctx["requisitos"] = get_requirements_by_tag(tag)
                ctx["requiere_auditoria_electrica"] = get_requires_diagnostic(tag)
                ctx["precio_extraido"] = "Precio bajo consulta"
                ctx["moneda_extraida"] = "GTQ"
                ctx["fuente_precio"] = "ontologia_local_sin_precio"
                ctx["fuente_detalle"] = {
                    "nivel": 3,
                    "fuente": "ontologia_local",
                    "producto_tag": tag,
                    "tiene_precio": False
                }
                if not ctx.get("checklist_universal"):
                    ctx["checklist_universal"] = inicializar_checklist_universal(ctx)
                checklist = ctx["checklist_universal"]
                checklist["productos_interes"] = "completado"
                ctx["checklist_universal"] = checklist
                logger.info("[SELECTOR] 📋 ✅ Información guardada en contexto (fuente: Ontología Local)")
                logger.info("=" * 80)
                return {"contexto_tecnico": ctx}
            else:
                ctx["error_ontologia"] = "Producto no encontrado en ontología"
        except Exception as e:
            logger.error(f"[SELECTOR] 📋 ❌ Error en Ontología Local: {e}")
            ctx["error_ontologia"] = str(e)

        # ================================================================
        # NIVEL 4: Derivación a Asesor (Último Recurso)
        # ================================================================
        logger.warning("[SELECTOR] 👤 NIVEL 4: Derivación a Asesor - Todos los niveles fallaron")
        ctx["derivation_reason"] = {
            "odoo_db": ctx.get("error_odoo_db", "No se intentó"),
            "web_scraping": ctx.get("error_web_scraping", "No se intentó"),
            "ontologia": ctx.get("error_ontologia", "No se intentó"),
            "timestamp": str(datetime.now(timezone.utc)),
            "consulta_usuario": ultimo
        }
        logger.info(f"[SELECTOR] 👤 Fallos: Odoo={ctx['derivation_reason']['odoo_db']}, Web={ctx['derivation_reason']['web_scraping']}, Ontología={ctx['derivation_reason']['ontologia']}")
        ctx["escalation_mode"] = True
        ctx["derivation_offered"] = True
        ctx["micdp_offered"] = True

        mensaje_derivacion = (
            "Lamento informarle que no hemos podido obtener información del producto solicitado a través de nuestras fuentes de datos.\n\n"
            "⚠️ He intentado obtener la información desde:\n"
            "  1. Nuestra base de datos de productos (Odoo) - Sin éxito\n"
            "  2. Nuestro catálogo web - Sin éxito\n"
            "  3. Nuestro catálogo interno - Sin éxito\n\n"
            "Para brindarle una atención personalizada y precisa, un asesor especializado de AISA Solar se comunicará con usted.\n\n"
            "¿Autoriza que un asesor le contacte para ayudarle con su requerimiento?"
        )

        new_messages = state.get("messages", []) + [AIMessage(content=mensaje_derivacion)]
        logger.info("[SELECTOR] 👤 ✅ Derivación a asesor activada.")
        logger.info("=" * 80)
        return {"messages": new_messages, "contexto_tecnico": ctx}

    # -------------------------------------------------------------------------
    # NODO 4: PROCESAR SELECCIÓN DE PRODUCTO (cuando el usuario elige una opción)
    # -------------------------------------------------------------------------
    @auditar_fase(nombre_fase="Procesar Selección de Producto", criticidad="MEDIA")
    @observe_node(node_name="procesar_seleccion_producto")
    async def procesar_seleccion_producto_node(state: AgentState):
        ctx = dict(state.get("contexto_tecnico") or {})
        ultimo = extraer_intencion_humana(state.get("messages", []))
        opciones = ctx.get("productos_opciones", [])
        if not opciones:
            ctx["esperando_seleccion"] = False
            return {"contexto_tecnico": ctx}

        match = re.search(r'\b([1-5])\b', ultimo)
        if match:
            idx = int(match.group(1)) - 1
            if 0 <= idx < len(opciones):
                producto_elegido = opciones[idx][2]
                ctx.pop("esperando_seleccion", None)
                ctx.pop("productos_opciones", None)
                ctx["product_tag"] = f"odoo_{producto_elegido['id']}"
                ctx["fuente_producto"] = "odoo"
                ctx["producto_odoo"] = producto_elegido
                ctx["tipo_producto"] = "unitario"
                ctx["requiere_auditoria_electrica"] = False
                ctx["requisitos"] = []
                ctx["precio_extraido"] = odoo_db_client.format_price(producto_elegido.get('list_price'))
                ctx["fuente_precio"] = "odoo_db"
                ctx["fuente_detalle"] = {
                    "nivel": 1,
                    "fuente": "odoo_db",
                    "producto_id": producto_elegido.get('id'),
                    "precio_raw": producto_elegido.get('list_price')
                }
                if not ctx.get("checklist_universal"):
                    ctx["checklist_universal"] = inicializar_checklist_universal(ctx)
                checklist = ctx["checklist_universal"]
                checklist["productos_interes"] = "completado"
                ctx["checklist_universal"] = checklist

                respuesta = f"✅ Excelente elección. El producto **{producto_elegido['name']}** tiene un precio de {ctx['precio_extraido']}. ¿Necesita más información o desea cotizar?"
                logger.info(f"[SELECTOR] 🎯 Usuario seleccionó opción {idx+1}: {producto_elegido['name']}")
                return {"messages": [AIMessage(content=respuesta)], "contexto_tecnico": ctx}
            else:
                return {"messages": [AIMessage(content="⚠️ Número inválido. Por favor, indique un número del 1 al 5.")], "contexto_tecnico": ctx}
        else:
            consulta_norm = normalizar_consulta(ultimo)
            if consulta_norm:
                mejores = []
                for idx, (orig_idx, score, prod) in enumerate(opciones):
                    texto = prod.get('name', '')
                    if prod.get('description_sale'):
                        texto += " " + prod['description_sale']
                    score = difflib.SequenceMatcher(None, consulta_norm, texto).ratio() * 100
                    mejores.append((idx, score, prod))
                mejores.sort(key=lambda x: x[1], reverse=True)
                if mejores and mejores[0][1] >= 80:
                    producto_elegido = mejores[0][2]
                    ctx.pop("esperando_seleccion", None)
                    ctx.pop("productos_opciones", None)
                    ctx["product_tag"] = f"odoo_{producto_elegido['id']}"
                    ctx["fuente_producto"] = "odoo"
                    ctx["producto_odoo"] = producto_elegido
                    ctx["tipo_producto"] = "unitario"
                    ctx["requiere_auditoria_electrica"] = False
                    ctx["requisitos"] = []
                    ctx["precio_extraido"] = odoo_db_client.format_price(producto_elegido.get('list_price'))
                    ctx["fuente_precio"] = "odoo_db"
                    ctx["fuente_detalle"] = {
                        "nivel": 1,
                        "fuente": "odoo_db",
                        "producto_id": producto_elegido.get('id'),
                        "precio_raw": producto_elegido.get('list_price')
                    }
                    if not ctx.get("checklist_universal"):
                        ctx["checklist_universal"] = inicializar_checklist_universal(ctx)
                    checklist = ctx["checklist_universal"]
                    checklist["productos_interes"] = "completado"
                    ctx["checklist_universal"] = checklist

                    respuesta = f"✅ Entendido, ha seleccionado **{producto_elegido['name']}** - Precio: {ctx['precio_extraido']}. ¿Necesita más información o desea cotizar?"
                    return {"messages": [AIMessage(content=respuesta)], "contexto_tecnico": ctx}

            return {"messages": [AIMessage(content="⚠️ No he entendido su selección. Por favor, indique el número de la opción que le interesa (1, 2, 3, 4 o 5).")], "contexto_tecnico": ctx}

    # -------------------------------------------------------------------------
    # NODO 5: CÁLCULO DE CARGA OFF-GRID
    # -------------------------------------------------------------------------
    @auditar_fase(nombre_fase="Cálculo de Carga Off‑Grid", criticidad="ALTA")
    @observe_node(node_name="calcular_carga_offgrid")
    async def calcular_carga_offgrid_node(state: AgentState):
        ctx = dict(state.get("contexto_tecnico") or {})
        topologia = ctx.get("topologia", "")
        if "OFF-GRID" not in topologia.upper():
            return {"contexto_tecnico": ctx}
        if ctx.get("calculo_carga_completado"):
            return {"contexto_tecnico": ctx}
        tag = ctx.get("product_tag")
        if not tag:
            return {"contexto_tecnico": ctx}
        dimensionamiento = get_dimensionamiento_by_tag(tag)
        if not dimensionamiento:
            return {"contexto_tecnico": ctx}
        if not ctx.get("equipos_usuario"):
            pregunta = "Para dimensionar su sistema Off‑Grid, ¿qué equipos planea usar y cuántas horas al día? (ej. Nevera 24h, TV 6h, bombillas 8h)"
            new_messages = state.get("messages", []) + [AIMessage(content=pregunta)]
            return {"messages": new_messages, "contexto_tecnico": ctx}
        return {"contexto_tecnico": ctx}

    # -------------------------------------------------------------------------
    # NODO 6: GENERAR RESPUESTA COMERCIAL (CON SUPERVISIÓN Y DETECCIÓN ACADÉMICA)
    # -------------------------------------------------------------------------
    @auditar_fase(nombre_fase="Generación de Respuesta Comercial", criticidad="ALTA")
    @observe_node(node_name="generar_respuesta_comercial")
    async def generar_respuesta_comercial_node(state: AgentState, config: RunnableConfig):
        ctx = dict(state.get("contexto_tecnico") or {})
        ultimo_mensaje = extraer_intencion_humana(state.get("messages", []))

        # --- DETECCIÓN DE CONSULTA ACADÉMICA ---
        keywords_academic = [
            "base académica", "referencias apa", "fuente de información", "base académica",
            "metodología", "marco teórico", "fundamento científico", "bibliografía",
            "qué método", "cómo funciona", "investigación", "tesis", "artículo",
            "publicación", "estudio", "qué dice la teoría", "referencias bibliográficas"
        ]
        if any(k in ultimo_mensaje for k in keywords_academic):
            academic_text = get_academic_framework()
            return {"messages": [AIMessage(content=academic_text)], "contexto_tecnico": ctx}
        # --- FIN DETECCIÓN ---

        # --- Extracción de datos (existente) ---
        if ultimo_mensaje:
            num_match = re.search(r'(\+?[0-9]{1,3}[-.\s]?)?[0-9]{4,10}', ultimo_mensaje)
            if num_match:
                raw_num = num_match.group(0)
                _, num_norm = normalizar_contacto("", raw_num, ctx.get("ciudad", ""))
                if num_norm and num_norm != "Pendiente":
                    ctx["whatsapp"] = num_norm
            name_match = re.search(r'(?:mi\s+nombre\s+es|nombre[:]\s*|me\s+llamo|soy\s+)([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)',
                                   ultimo_mensaje, re.IGNORECASE)
            if name_match:
                raw_name = name_match.group(1).strip()
                if raw_name and len(raw_name) > 1:
                    ctx["nombre"] = raw_name

        if ultimo_mensaje and (not ctx.get("nombre") or ctx.get("nombre") == "Usuario" or not ctx.get("whatsapp")):
            try:
                prompt_text = get_prompt("jarvi_extractor_contacto", mensaje=ultimo_mensaje)
                extraccion = await extractor_llm.ainvoke(prompt_text)
                if extraccion.nombre and (not ctx.get("nombre") or ctx["nombre"] == "Usuario"):
                    ctx["nombre"] = extraccion.nombre
                if extraccion.telefono and (not ctx.get("whatsapp") or ctx["whatsapp"] == "Pendiente"):
                    ctx["whatsapp"] = extraccion.telefono
            except Exception:
                pass

        if ultimo_mensaje and not ctx.get("vendedor"):
            vendedor_match = re.search(r'(?:mi\s+vendedor\s+es|vendedor[:]\s*)([A-Za-z0-9\s]+)',
                                       ultimo_mensaje, re.IGNORECASE)
            if vendedor_match:
                ctx["vendedor"] = vendedor_match.group(1).strip()

        if ultimo_mensaje and not ctx.get("tipo_producto"):
            tipo = extraer_tipo_producto(ultimo_mensaje)
            if tipo:
                ctx["tipo_producto"] = tipo

        if ctx.get("topologia") and ctx.get("tipo_producto") and not ctx.get("productos_interes"):
            productos = obtener_productos_relevantes(
                topologia=ctx["topologia"],
                tipo=ctx["tipo_producto"],
                max_items=5
            )
            ctx["productos_interes"] = normalizar_productos_interes(productos)

        if not ctx.get("checklist_universal"):
            ctx["checklist_universal"] = inicializar_checklist_universal(ctx)
        checklist = ctx["checklist_universal"]

        for campo in CAMPOS_SCORE_UNIVERSAL:
            valor = ctx.get(campo)
            if campo == "productos_interes" and isinstance(valor, list) and valor:
                checklist[campo] = "completado"
            elif campo == "tipo_producto" and valor:
                checklist[campo] = "completado"
            elif campo == "vendedor" and valor:
                checklist[campo] = "completado"
            elif campo == "calculo_carga_completado" and valor:
                checklist[campo] = "completado"
            elif campo == "requiere_auditoria_electrica" and valor is not None:
                checklist[campo] = "completado"
            elif isinstance(valor, str) and valor and valor != "Pendiente":
                checklist[campo] = "completado"
            elif isinstance(valor, float) and valor > 0:
                checklist[campo] = "completado"
            elif checklist.get(campo) != "completado":
                checklist[campo] = "pendiente"
        ctx["checklist_universal"] = checklist

        score = calcular_puntaje_completitud(ctx)
        ctx["score_actual"] = score
        logger.info(f"Score actual: {score}%")

        pendientes = [campo for campo, status in checklist.items() if status == "pendiente"]
        if pendientes:
            prioridad = ["topologia", "tipo_producto", "productos_interes", "departamento", "municipio", "ciudad",
                         "empresa_electrica", "tarifa_base_gtq", "consumo_mensual_kwh", "vendedor"]
            pendientes_ordenados = sorted(pendientes, key=lambda x: prioridad.index(x) if x in prioridad else 99)
            preguntas = []
            for campo in pendientes_ordenados:
                pregunta = f"¿Cuál es su {campo.replace('_',' ')}?"
                if ctx.get("requisitos"):
                    for req in ctx.get("requisitos", []):
                        if req.get("field") == campo:
                            pregunta = req.get("question", pregunta)
                            break
                preguntas.append(pregunta)
            regla_datos = f"1. DEBES recopilar sutilmente: {', '.join(preguntas)}."
        else:
            regla_datos = "Ya tienes toda la información técnica. Enfócate en ofrecer una solución y cerrar la conversación."

        ontologia_dinamica = obtener_fragmento_ontologia(ctx.get('topologia'))

        nombre_ctx = ctx.get("nombre", "Usuario")
        whatsapp_ctx = ctx.get("whatsapp", "Pendiente")
        nombre_run, whatsapp_run = normalizar_contacto(nombre_ctx, whatsapp_ctx, ctx.get("ciudad", ""))

        if "metadata" not in config:
            config["metadata"] = {}
        config["metadata"]["whatsapp"] = whatsapp_run
        config["metadata"]["topologia"] = ctx.get("topologia", "Desconocida")
        if ctx.get("tipo_producto"):
            config["metadata"]["tipo_producto"] = ctx["tipo_producto"]
        productos_normalizados = normalizar_productos_interes(ctx.get("productos_interes", []))
        config["metadata"]["productos_tags"] = [p.get("tag") for p in productos_normalizados if p.get("tag")]

        conocimiento_usuario = ""
        if ctx.get("nombre") and ctx.get("nombre") != "Usuario":
            conocimiento_usuario += f"El usuario se llama {ctx['nombre']}. "
        if ctx.get("ciudad"):
            conocimiento_usuario += f"Vive en {ctx['ciudad']}. "
        if ctx.get("consumo_mensual_kwh"):
            conocimiento_usuario += f"Consume {ctx['consumo_mensual_kwh']} kWh al mes. "
        if ctx.get("numero_personas"):
            conocimiento_usuario += f"Necesita agua caliente para {ctx['numero_personas']} personas. "
        if ctx.get("product_tag"):
            ontologia = cargar_ontologia()
            item = ontologia.get(ctx["product_tag"], {})
            if item.get("nombre"):
                conocimiento_usuario += f"Está interesado en {item['nombre']}. "
        if ctx.get("productos_interes"):
            nombres = [p.get("nombre") for p in productos_normalizados if p.get("nombre")]
            if nombres:
                conocimiento_usuario += f"Productos de interés: {', '.join(nombres)}. "
        if not conocimiento_usuario:
            conocimiento_usuario = "No se tiene información previa del usuario."

        prompt_content = get_prompt(
            "jarvi_system_prompt",
            ciudad=ctx.get('ciudad', 'PENDIENTE'),
            empresa_electrica=ctx.get('empresa_electrica', 'PENDIENTE'),
            tarifa_base_gtq=ctx.get('tarifa_base_gtq', 'PENDIENTE'),
            regla_datos=regla_datos,
            ontologia_dinamica=ontologia_dinamica,
            conocimiento_usuario=conocimiento_usuario
        )
        prompt_sistema = SystemMessage(content=prompt_content)

        messages_limit = state["messages"][-10:] if len(state["messages"]) > 10 else state["messages"]
        respuesta = await llm.ainvoke([prompt_sistema] + messages_limit, config=config)

        respuesta_final = respuesta.content

        # =========================================================================
        # SI MICDP ESTÁ ACTIVO, SALTAR EL SUPERVISOR COMPLETAMENTE
        # =========================================================================
        if ctx.get("micdp_active", False):
            logger.info("MICDP activo: saltando evaluación del supervisor para preservar integridad de la entrevista.")
            respuesta.content = respuesta_final
            return {"messages": [respuesta], "contexto_tecnico": ctx}

        # =========================================================================
        # APLICAR SUPERVISIÓN (FLUJO NORMAL)
        # =========================================================================
        eval_data = {
            "response": respuesta_final,
            "contexto": ctx,
            "messages": state["messages"],
            "output_type": "response",
            "user_message": ultimo_mensaje,
            "price_extractor_failed": False,
            "fuente_producto": ctx.get("fuente_producto")
        }
        evaluacion = _supervisor.evaluate(eval_data)

        mensaje_base = "Disculpe, esa información específica no está disponible en este momento."
        opciones = " ¿Prefiere que un asesor de AISA Solar le contacte para brindarle una atención personalizada? o desea iniciar el **Proceso Conversacional para la Definición de Proyectos** (responda 'Sí' para iniciar el proceso, o 'Asesor' para contacto humano)"

        if evaluacion["decision"] == "rewrite":
            if evaluacion.get("modified_response"):
                respuesta_final = evaluacion["modified_response"]
                if "asesor" in respuesta_final.lower() or "llamada" in respuesta_final.lower():
                    respuesta_final = f"{mensaje_base} {opciones}"
                    ctx["derivation_offered"] = True
                    ctx["micdp_offered"] = True
                    logger.info(f"Supervisor reescribió a derivación; se reemplaza por oferta de MICDP.")
                else:
                    logger.info(f"Supervisor reescribió respuesta: {evaluacion['rule_id']}")
        elif evaluacion["decision"] == "block":
            respuesta_final = f"{mensaje_base} {opciones}"
            ctx["derivation_offered"] = True
            ctx["micdp_offered"] = True
            logger.warning(f"Supervisor bloqueó respuesta: {evaluacion['rule_id']}. Se ofrecen ambas opciones.")
        elif evaluacion["decision"] == "rewrite_context":
            if evaluacion.get("modified_context"):
                ctx.update(evaluacion["modified_context"])
                logger.info(f"Supervisor modificó contexto: {evaluacion['rule_id']}")
        elif evaluacion["decision"] == "force_fallback":
            ctx["escalation_mode"] = True
            respuesta_final = f"{mensaje_base} {opciones}"
            ctx["derivation_offered"] = True
            ctx["micdp_offered"] = True
            logger.info(f"Supervisor activó modo escalación: {evaluacion['rule_id']}. Se ofrecen ambas opciones.")
        elif evaluacion["decision"] == "force_closure":
            if evaluacion.get("modified_response"):
                respuesta_final = evaluacion["modified_response"]
                logger.info(f"Supervisor forzó cierre: {evaluacion['rule_id']}")
        elif evaluacion["decision"] == "end_conversation":
            respuesta_final = evaluacion.get("modified_response", "Gracias por contactar a AISA Solar. ¡Que tenga un excelente día!")
            ctx["conversation_end"] = True
            logger.info(f"Supervisor finalizó conversación: {evaluacion['rule_id']}")

        respuesta.content = respuesta_final
        return {"messages": [respuesta], "contexto_tecnico": ctx}

    # -------------------------------------------------------------------------
    # NODO 7: ACTUALIZAR CHECKLIST
    # -------------------------------------------------------------------------
    @auditar_fase(nombre_fase="Actualización Semántica de Checklist", criticidad="ALTA")
    @observe_node(node_name="actualizar_checklist")
    async def actualizar_checklist_node(state: AgentState):
        ctx = dict(state.get("contexto_tecnico") or {})
        messages = state.get("messages", [])

        if ctx.get("micdp_active", False):
            return {"contexto_tecnico": ctx}

        MAX_MESSAGES_FOR_EXTRACTION = 20
        if len(messages) > MAX_MESSAGES_FOR_EXTRACTION:
            first_msgs = messages[:5]
            last_msgs = messages[-15:]
            messages_for_extract = first_msgs + last_msgs
            logger.info(f"Truncando historial: {len(messages)} → {len(messages_for_extract)} mensajes para extracción")
        else:
            messages_for_extract = messages

        historial_texto = ""
        for msg in messages_for_extract:
            if isinstance(msg, HumanMessage):
                historial_texto += f"Usuario: {msg.content}\n"
            elif isinstance(msg, AIMessage):
                historial_texto += f"Asistente: {msg.content}\n"

        if not historial_texto:
            return {"contexto_tecnico": ctx}

        tag_info = ""
        if ctx.get("product_tag"):
            ontologia = cargar_ontologia()
            item = ontologia.get(ctx["product_tag"], {})
            tag_info = f"Producto detectado: {item.get('nombre', '')} (tag {ctx['product_tag']})"

        prompt_extract = f"""
        Extrae los siguientes campos de la conversación. Si no se mencionan, déjalos como null.
        Usa solo la información explícitamente mencionada por el usuario.
        No inventes datos.

        {tag_info}

        Historial de la conversación:
        {historial_texto}

        Campos a extraer:
        - nombre: Nombre completo del cliente.
        - whatsapp: Número de teléfono (formato E.164, ej. +50212345678).
        - departamento: Departamento de Guatemala.
        - municipio: Municipio de Guatemala.
        - ciudad: Ciudad o localidad.
        - empresa_electrica: Empresa distribuidora (EEGSA, DEOCSA, etc.).
        - tarifa_base_gtq: Tarifa eléctrica en GTQ/kWh (número).
        - topologia: On-Grid, Off-Grid, o No aplica.
        - calculo_carga_completado: true/false.
        - requiere_auditoria_electrica: true/false.
        - vendedor: Nombre del vendedor asignado.
        - tipo_producto: sistema o unitario.
        - productos_interes: Lista de nombres de productos mencionados.
        """

        try:
            extraccion: ChecklistExtract = await checklist_llm.ainvoke(prompt_extract)
        except Exception as e:
            logger.error(f"Error en extracción semántica: {e}")
            return {"contexto_tecnico": ctx}

        if extraccion.nombre and not ctx.get("nombre"):
            ctx["nombre"] = extraccion.nombre
        if extraccion.whatsapp and not ctx.get("whatsapp"):
            ctx["whatsapp"] = extraccion.whatsapp
        if extraccion.departamento and not ctx.get("departamento"):
            ctx["departamento"] = extraccion.departamento
        if extraccion.municipio and not ctx.get("municipio"):
            ctx["municipio"] = extraccion.municipio
        if extraccion.ciudad and not ctx.get("ciudad"):
            ctx["ciudad"] = extraccion.ciudad
        if extraccion.empresa_electrica and not ctx.get("empresa_electrica"):
            ctx["empresa_electrica"] = extraccion.empresa_electrica
        if extraccion.tarifa_base_gtq and not ctx.get("tarifa_base_gtq"):
            ctx["tarifa_base_gtq"] = extraccion.tarifa_base_gtq
        if extraccion.topologia and not ctx.get("topologia"):
            ctx["topologia"] = extraccion.topologia
        if extraccion.calculo_carga_completado is not None and not ctx.get("calculo_carga_completado"):
            ctx["calculo_carga_completado"] = extraccion.calculo_carga_completado
        if extraccion.requiere_auditoria_electrica is not None and not ctx.get("requiere_auditoria_electrica"):
            ctx["requiere_auditoria_electrica"] = extraccion.requiere_auditoria_electrica
        if extraccion.vendedor and not ctx.get("vendedor"):
            ctx["vendedor"] = extraccion.vendedor
        if extraccion.tipo_producto and not ctx.get("tipo_producto"):
            ctx["tipo_producto"] = extraccion.tipo_producto

        if extraccion.productos_interes:
            ctx["productos_interes"] = normalizar_productos_interes(extraccion.productos_interes)

        if not ctx.get("checklist_universal"):
            ctx["checklist_universal"] = inicializar_checklist_universal(ctx)
        checklist = ctx["checklist_universal"]

        for campo in CAMPOS_SCORE_UNIVERSAL:
            valor = ctx.get(campo)
            if campo == "productos_interes" and isinstance(valor, list) and valor:
                checklist[campo] = "completado"
            elif campo == "tipo_producto" and valor:
                checklist[campo] = "completado"
            elif campo == "vendedor" and valor:
                checklist[campo] = "completado"
            elif campo == "calculo_carga_completado" and valor:
                checklist[campo] = "completado"
            elif campo == "requiere_auditoria_electrica" and valor is not None:
                checklist[campo] = "completado"
            elif isinstance(valor, str) and valor and valor != "Pendiente":
                checklist[campo] = "completado"
            elif isinstance(valor, float) and valor > 0:
                checklist[campo] = "completado"
            elif checklist.get(campo) != "completado":
                checklist[campo] = "pendiente"

        ctx["checklist_universal"] = checklist
        score = calcular_puntaje_completitud(ctx)
        ctx["score_actual"] = score
        logger.info(f"Score tras extracción semántica: {score}%")

        return {"contexto_tecnico": ctx}

    # -------------------------------------------------------------------------
    # NODO 8: VERIFICAR CIERRE
    # -------------------------------------------------------------------------
    @auditar_fase(nombre_fase="Verificación de Cierre Comercial", criticidad="ALTA")
    @observe_node(node_name="verificar_cierre")
    async def verificar_cierre_node(state: AgentState, config: RunnableConfig):
        ctx = dict(state.get("contexto_tecnico") or {})
        if ctx.get("micdp_active", False):
            return {"contexto_tecnico": ctx}

        score = ctx.get("score_actual", 0.0)
        messages = state.get("messages", [])

        if score < 60.0:
            return {"contexto_tecnico": ctx}

        if ctx.get("cierre_realizado"):
            return {"contexto_tecnico": ctx}

        precio_texto = ""
        tag = ctx.get("product_tag")
        if tag:
            try:
                precio_data = get_precio_by_tag(tag)
                if precio_data and precio_data.get("precio"):
                    precio = precio_data["precio"]
                    moneda = precio_data.get("moneda", "GTQ")
                    precio_texto = f"{precio:,.2f} {moneda}"
                else:
                    precio_texto = "disponible bajo consulta"
            except Exception as e:
                logger.error(f"Error al obtener precio para tag {tag}: {e}")
                precio_texto = "disponible bajo consulta"
        else:
            precio_texto = "disponible bajo consulta"

        nombre_producto = ""
        if tag:
            ontologia = cargar_ontologia()
            item = ontologia.get(tag, {})
            nombre_producto = item.get("nombre", "el producto")

        resumen = f"Resumen de su solución: {nombre_producto} con un costo aproximado de {precio_texto}."
        advertencia = "Le recuerdo que este precio no incluye instalación, mano de obra, servicios adicionales ni costos de envío."

        preguntas = [
            f"{resumen} {advertencia}",
            "¿Cómo visualiza esta solución para su caso?",
            "Para poder coordinar la entrega e instalación, ¿qué fecha estimada le gustaría tener el equipo operativo?",
            "Actualmente, ¿tiene un vendedor asignado? Si no es así, ¿le gustaría que uno de nuestro equipo lo contacte?"
        ]

        for pregunta in preguntas:
            messages.append(AIMessage(content=pregunta))

        ctx["cierre_realizado"] = True

        return {
            "messages": messages,
            "contexto_tecnico": ctx
        }

    # -------------------------------------------------------------------------
    # NODO 9: ANEXAR CASO
    # -------------------------------------------------------------------------
    @observe_node(node_name="anexar_caso_respuesta")
    async def anexar_caso_respuesta_node(state: AgentState, config: RunnableConfig):
        messages = state.get("messages", [])
        caso = config.get("metadata", {}).get("caso", "000000000000")
        if messages and isinstance(messages[-1], AIMessage):
            last_msg = messages[-1]
            if not last_msg.content.endswith(f"[Caso No. {caso}]"):
                new_content = f"{last_msg.content} [Caso No. {caso}]"
                messages[-1] = AIMessage(content=new_content, additional_kwargs=last_msg.additional_kwargs)
        return {"messages": messages}

    # -------------------------------------------------------------------------
    # NODO 10: EJECUTAR ENTREVISTA (MICDP)
    # -------------------------------------------------------------------------
    @auditar_fase(nombre_fase="Ejecución de Entrevista MICDP", criticidad="ALTA")
    @observe_node(node_name="ejecutar_entrevista")
    async def ejecutar_entrevista_node(state: AgentState, config: RunnableConfig):
        ctx = dict(state.get("contexto_tecnico") or {})
        ultimo = extraer_intencion_humana(state.get("messages", []))
        thread_id = config.get("configurable", {}).get("thread_id", "unknown")

        if ctx.get("micdp_active", False):
            asesor_keywords = ["asesor", "vendedor", "llamada", "hablar con", "contactar", "humano", "ayuda real"]
            if any(k in ultimo for k in asesor_keywords):
                logger.info(f"Usuario solicita asesor durante MICDP, finalizando proceso.")
                ctx["micdp_active"] = False
                ctx["conversation_end"] = True
                respuesta = "Entendido, derivaré su solicitud a un asesor especializado de AISA Solar. En breve se pondrá en contacto con usted. ¡Que tenga un excelente día!"
                return {"messages": [AIMessage(content=respuesta)], "contexto_tecnico": ctx}

        orquestador = await get_epistemology()
        if orquestador is None:
            return {"messages": [AIMessage(content="Error: orquestador no inicializado. Contacte a soporte.")]}

        if not ctx.get("micdp_active", False):
            ctx["micdp_active"] = True
            ctx["micdp_accepted"] = False
            ctx["micdp_offered"] = False

        definition = await orquestador.repo.get_definition(thread_id)
        if not definition:
            nombre = ctx.get("nombre", "Usuario")
            welcome = orquestador._get_welcome()
            await orquestador.repo.create_definition(thread_id)
            if nombre:
                await orquestador.repo.update_state(thread_id, "IDENTIDAD", {"variables": {"nombre": nombre}})
            return {"messages": [AIMessage(content=welcome)], "contexto_tecnico": ctx}

        try:
            result = await orquestador.process_message(thread_id, ultimo)
        except Exception as e:
            logger.error(f"Excepción en process_message: {e}")
            result = None

        if result is None:
            result = {
                "action": "error",
                "response": "Lo siento, hubo un problema procesando su mensaje. Por favor, intente de nuevo."
            }

        action = result.get("action", "question")
        response = result.get("response", "Continuemos.")
        usage = result.get("usage", {})

        ai_message = AIMessage(content=response)
        if usage:
            ai_message.response_metadata = {
                "token_usage": {
                    "prompt_tokens": usage.get("input", 0),
                    "completion_tokens": usage.get("output", 0),
                    "total_tokens": usage.get("total", 0)
                }
            }

        if action == "completed" or action == "handoff":
            ctx["conversation_end"] = True
            ctx["micdp_active"] = False

        return {"messages": [ai_message], "contexto_tecnico": ctx}

    # -------------------------------------------------------------------------
    # CONDICIÓN DE BORDE
    # -------------------------------------------------------------------------
    def my_tools_condition(state: AgentState):
        messages = state.get("messages", [])
        ctx = state.get("contexto_tecnico", {})
        ultimo = extraer_intencion_humana(messages)

        if ctx.get("micdp_active", False):
            return "ejecutar_entrevista"
        if ctx.get("derivation_offered") and any(k in ultimo for k in ["sí", "si", "claro", "adelante"]):
            return "ejecutar_entrevista"
        if ctx.get("derivation_offered") and any(k in ultimo for k in ["asesor", "llamada", "contactar"]):
            return "actualizar_checklist"

        if ctx.get("esperando_seleccion"):
            return "procesar_seleccion_producto"

        if messages and isinstance(messages[-1], AIMessage) and messages[-1].tool_calls:
            return "tools"
        return "actualizar_checklist"

    # -------------------------------------------------------------------------
    # ENSAMBLAJE DEL GRAFO
    # -------------------------------------------------------------------------
    graph_builder.add_node("clasificar_intencion_comercial", clasificar_intencion_comercial_node)
    graph_builder.add_node("validar_ubicacion_cliente", validar_ubicacion_cliente_node)
    graph_builder.add_node("seleccionar_productos", seleccionar_productos_node)
    graph_builder.add_node("procesar_seleccion_producto", procesar_seleccion_producto_node)
    graph_builder.add_node("calcular_carga_offgrid", calcular_carga_offgrid_node)
    graph_builder.add_node("generar_respuesta_comercial", generar_respuesta_comercial_node)
    graph_builder.add_node("actualizar_checklist", actualizar_checklist_node)
    graph_builder.add_node("verificar_cierre", verificar_cierre_node)
    graph_builder.add_node("anexar_caso_respuesta", anexar_caso_respuesta_node)
    graph_builder.add_node("ejecutar_entrevista", ejecutar_entrevista_node)
    graph_builder.add_node("tools", ToolNode([procesar_oportunidad_backend]))

    graph_builder.add_edge(START, "clasificar_intencion_comercial")
    graph_builder.add_edge("clasificar_intencion_comercial", "validar_ubicacion_cliente")
    graph_builder.add_edge("validar_ubicacion_cliente", "seleccionar_productos")
    graph_builder.add_edge("seleccionar_productos", "calcular_carga_offgrid")
    graph_builder.add_edge("calcular_carga_offgrid", "generar_respuesta_comercial")
    graph_builder.add_conditional_edges("generar_respuesta_comercial", my_tools_condition)
    graph_builder.add_edge("tools", "actualizar_checklist")
    graph_builder.add_edge("actualizar_checklist", "verificar_cierre")
    graph_builder.add_edge("verificar_cierre", "anexar_caso_respuesta")
    graph_builder.add_edge("ejecutar_entrevista", END)

    graph_builder.add_edge("procesar_seleccion_producto", "generar_respuesta_comercial")

    return graph_builder.compile(checkpointer=checkpointer)

if __name__ == "__main__":
    from langgraph.checkpoint.memory import MemorySaver
    checkpointer_studio = MemorySaver()
    jarvi_graph = create_graph(checkpointer_studio)
else:
    jarvi_graph = None
