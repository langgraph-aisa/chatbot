"""
agent_graph.py
Módulo central del grafo agéntico de JARVI 2.0.
Contiene la definición del estado compartido, herramientas, nodos de razonamiento
y la función de construcción del grafo con persistencia en PostgreSQL.
Incorpora el decorador CTFOM de telemetría cognitiva para la observabilidad
de cada nodo del grafo (trazas, latencia, errores).

Estándares: ISO/IEC/IEEE 12207, ISO/IEC 26514, ISO/IEC 25010, ISO/IEC 29119.
Pruebas de caja negra: BC‑T01 a BC‑T10 (ver anexo).
"""

import os
import time
import uuid
import threading
import requests
import re
import functools
import json
import logging
from typing import Annotated, TypedDict, Optional, List, Dict, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64
from pydantic import BaseModel, Field

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, START
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.base import BaseCheckpointSaver

import config
from audit import auditar_fase
from ontology import obtener_fragmento_ontologia, buscar_productos_por_mensaje
# --- CTFOM: módulo de telemetría cognitiva ---
from telemetry import trace_id_var, span_id_var, parent_span_id_var, schedule_telemetry_event

# === NUEVO: Importación del cliente de base de datos ===
from db_client import actualizar_thread, obtener_thread_por_whatsapp, registrar_evento_auditoria

# =============================================================================
# === NUEVO: CONFIGURACIÓN DE VENDEDORES ===
# =============================================================================
VENDEDORES_PATH = os.path.join(os.path.dirname(__file__), "vendedores.json")

def cargar_vendedores() -> List[Dict[str, Any]]:
    """Carga la lista de vendedores desde el archivo JSON."""
    try:
        with open(VENDEDORES_PATH, "r", encoding="utf-8") as f:
            return json.load(f).get("vendedores", [])
    except Exception as e:
        logging.getLogger("jarvi.agent").warning(f"Error al cargar vendedores: {e}")
        return [{"id": 1, "nombre": "Gerencia", "email": "gerencia@aisa.com.gt", "default": True}]

def asignar_vendedor(whatsapp: str = None) -> str:
    """Asigna un vendedor basado en el WhatsApp (si se proporciona) o retorna el default (gerencia)."""
    vendedores = cargar_vendedores()
    for v in vendedores:
        if v.get("default", False):
            return v["email"]
    return "gerencia@aisa.com.gt"

def validar_campos_obligatorios(ctx: dict) -> bool:
    """Verifica que todos los campos obligatorios estén presentes en el contexto."""
    required = ["nombre", "whatsapp", "email", "productos", "vendedor"]
    return all(ctx.get(field) for field in required)

# =============================================================================
# FIN DE LAS NUEVAS FUNCIONES
# =============================================================================

# ---------------------------------------------------------------------------
# Diccionario de códigos de área para Centroamérica
# ---------------------------------------------------------------------------
CODIGOS_AREA = {
    "belice": "+501", "costa rica": "+506", "el salvador": "+503",
    "guatemala": "+502", "honduras": "+504", "nicaragua": "+505",
    "panama": "+507", "panamá": "+507"
}


def normalizar_contacto(nombre_raw: str, whatsapp_raw: str, ubicacion_raw: str) -> tuple:
    """
    Normaliza el nombre, el número de WhatsApp y el código de área del cliente.
    """
    nombre_str = str(nombre_raw).strip() if nombre_raw else "Usuario"
    nombre_partes = nombre_str.split()
    nombre_normalizado = " ".join([p.capitalize() for p in nombre_partes]) if nombre_partes else "Usuario"

    codigo_area = "+502"
    ubicacion_lower = str(ubicacion_raw).lower() if ubicacion_raw else ""
    for pais, codigo in CODIGOS_AREA.items():
        if pais in ubicacion_lower:
            codigo_area = codigo
            break

    whatsapp_str = str(whatsapp_raw) if whatsapp_raw else ""
    digits = re.sub(r'\D', '', whatsapp_str)
    if not digits:
        whatsapp_formateado = "Pendiente"
    else:
        codigo_limpio = codigo_area.replace('+', '')
        if digits.startswith(codigo_limpio) and len(digits) > len(codigo_limpio) + 5:
            base = digits[len(codigo_limpio):]
        else:
            base = digits
        if len(base) >= 8:
            whatsapp_formateado = f"{codigo_area} {base[:4]}-{base[4:]}"
        else:
            whatsapp_formateado = f"{codigo_area} {base}"
    return nombre_normalizado, whatsapp_formateado


# ---------------------------------------------------------------------------
# Esquemas de datos
# ---------------------------------------------------------------------------
class ExtractorContacto(BaseModel):
    nombre: Optional[str] = Field(None, description="Nombre de pila y apellidos.")
    telefono: Optional[str] = Field(None, description="Número telefónico.")
    email: Optional[str] = Field(None, description="Correo electrónico.")


class InferenciaEnergetica(TypedDict):
    ciudad: Optional[str]
    empresa_electrica: Optional[str]
    tarifa_base_gtq: Optional[float]
    topologia: Optional[str]
    calculo_carga_completado: bool
    requiere_auditoria_electrica: bool
    nombre: Optional[str]
    whatsapp: Optional[str]
    email: Optional[str]
    productos: Optional[List[str]]
    vendedor: Optional[str]
    preguntando_whatsapp: Optional[bool]
    preguntando_email: Optional[bool]
    preguntando_productos: Optional[bool]


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    contexto_tecnico: InferenciaEnergetica


# ---------------------------------------------------------------------------
# Decorador CTFOM
# ---------------------------------------------------------------------------
def observe_node(layer: str = "graph", node_name: str = ""):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            trace_id = trace_id_var.get()
            span_id = str(uuid.uuid4())
            parent = span_id_var.get()
            span_id_var.set(span_id)

            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
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


# ---------------------------------------------------------------------------
# Herramienta de persistencia de oportunidades
# ---------------------------------------------------------------------------
@tool
@auditar_fase(nombre_fase="Herramienta Persistencia Oportunidades", criticidad="ALTA")
def procesar_oportunidad_backend(
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
    Registra una oportunidad comercial calificada y notifica al equipo de AISA.

    Recibe los datos del cliente, ubicacion, consumo, distribuidora, necesidad,
    equipos propuestos, WhatsApp y resumen ejecutivo. Normaliza el contacto,
    dispara notificaciones por correo/webhook en segundo plano y devuelve una
    confirmacion textual para el agente.
    """
    nombre_norm, whatsapp_norm = normalizar_contacto(nombre_apellidos, numero_whatsapp, departamento_municipio)

    def tarea_background():
        num_limpio = ''.join(filter(str.isdigit, whatsapp_norm))
        try:
            msg = MIMEMultipart()
            msg['To'] = config.CONTROLLER_EMAIL
            msg['From'] = config.SMTP_USER
            msg['Subject'] = resumen_18_palabras
            cuerpo = (
                f"Oportunidad Validada por Auditoría ISO:\n\n"
                f"Cliente: {nombre_norm}\nWhatsApp: {whatsapp_norm}\n"
                f"Ubicación: {departamento_municipio}\nConsumo: {consumo_actual}\n"
                f"Distribuidora: {empresa_electrica}\nEspecificación: {definicion_necesidad}\n\n"
                f"Equipos Propuestos:\n{listado_equipos_html}"
            )
            msg.attach(MIMEText(cuerpo, 'plain'))
            creds = Credentials(
                token=None,
                refresh_token=os.getenv("GMAIL_REFRESH_TOKEN"),
                token_uri="https://oauth2.googleapis.com/token",
                client_id=os.getenv("GMAIL_CLIENT_ID"),
                client_secret=os.getenv("GMAIL_CLIENT_SECRET")
            )
            service = build('gmail', 'v1', credentials=creds)
            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')
            service.users().messages().send(userId="me", body={'raw': raw}).execute()
        except Exception as e:
            print(f"Fallo en envío de correo: {e}")

        payload_wa = {
            "instance_id": os.getenv("APICHAT_INSTANCE", ""),
            "number": num_limpio,
            "text": (
                f"🚨 Lead Calificado:\n\n"
                f"Cliente: {nombre_norm}\nWhatsApp: {whatsapp_norm}\n"
                f"Ubicación: {departamento_municipio}\nEquipos:\n{listado_equipos_html}"
            )
        }
        try:
            requests.post(
                os.getenv("APICHAT_ENDPOINT", ""),
                json=payload_wa,
                headers={
                    "Authorization": f"Bearer {os.getenv('APICHAT_TOKEN', '')}",
                    "Content-Type": "application/json"
                },
                timeout=15
            )
        except Exception as e:
            print(f"Fallo en envío de webhook: {e}")

    threading.Thread(target=tarea_background).start()
    return f"✅ Los datos técnicos han sido guardados y auditados. Contacto: {whatsapp_norm}."


def extraer_intencion_humana(messages: list) -> str:
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            content = msg.content
            if isinstance(content, str):
                return content.lower()
            elif isinstance(content, list):
                textos = []
                for item in content:
                    if isinstance(item, dict) and "text" in item:
                        textos.append(item["text"])
                    elif isinstance(item, str):
                        textos.append(item)
                return " ".join(textos).lower()
    return ""


# ---------------------------------------------------------------------------
# Construcción del grafo
# ---------------------------------------------------------------------------
def create_graph(checkpointer: BaseCheckpointSaver):
    graph_builder = StateGraph(AgentState)

    # Modelo de lenguaje principal (GPT-4o mini) con reintentos para rate limit
    llm = ChatOpenAI(
        model="gpt-4o-mini",
        api_key=os.getenv("OPENAI_API_KEY1"),
        temperature=0.1,
        max_retries=5
    ).bind_tools([procesar_oportunidad_backend])

    extractor_llm = llm.with_structured_output(ExtractorContacto)

    # -------------------- Nodo: Clasificador Topológico --------------------
    @auditar_fase(nombre_fase="Clasificador Topológico", criticidad="MEDIA")
    @observe_node(node_name="clasificador_topologia")
    def clasificador_topologia_node(state: AgentState):
        ctx = dict(state.get("contexto_tecnico") or {})
        logger = logging.getLogger("jarvi.agent")
        logger.info(f"[clasificador] contexto recibido: {ctx}")
        ultimo = extraer_intencion_humana(state.get("messages", []))
        if not ultimo:
            logger.info(f"[clasificador] contexto después (sin cambios): {ctx}")
            return {"contexto_tecnico": ctx}
        if not ctx.get("topologia"):
            if any(k in ultimo for k in ["red", "atado", "interconectado", "ahorro", "eegsa", "factura"]):
                ctx["topologia"] = "On-Grid (Sistemas Atados a la Red)"
                ctx["requiere_auditoria_electrica"] = True
            elif any(k in ultimo for k in ["aislado", "batería", "bateria", "finca", "autónomo", "off-grid"]):
                ctx["topologia"] = "Off-Grid (Sistemas Aislados)"
                ctx["requiere_auditoria_electrica"] = True
        logger.info(f"[clasificador] contexto después: {ctx}")
        return {"contexto_tecnico": ctx}

    # -------------------- Nodo: Validador Geográfico --------------------
    @auditar_fase(nombre_fase="Validador Geográfico", criticidad="MEDIA")
    @observe_node(node_name="validador_geolocalizacion")
    def validador_geolocalizacion_node(state: AgentState):
        ctx = dict(state.get("contexto_tecnico") or {})
        logger = logging.getLogger("jarvi.agent")
        logger.info(f"[validador] contexto recibido: {ctx}")
        ultimo = extraer_intencion_humana(state.get("messages", []))
        if not ultimo:
            logger.info(f"[validador] contexto después (sin cambios): {ctx}")
            return {"contexto_tecnico": ctx}
        if not ctx.get("ciudad"):
            if any(k in ultimo for k in ["guatemala", "mixco", "capital", "ciudad", "villa nueva"]):
                ctx["ciudad"] = "Guatemala Metropolitana"
                if ctx.get("requiere_auditoria_electrica"):
                    ctx["empresa_electrica"] = "EEGSA"
                    ctx["tarifa_base_gtq"] = 1.45
        logger.info(f"[validador] contexto después: {ctx}")
        return {"contexto_tecnico": ctx}

    # -------------------- Nodo: Chatbot (extracción inmediata) --------------------
    @auditar_fase(nombre_fase="Inferencia del Chatbot", criticidad="ALTA")
    @observe_node(node_name="chatbot")
    def chatbot_node(state: AgentState, config: RunnableConfig):
        ctx = dict(state.get("contexto_tecnico") or {})
        ultimo_mensaje = extraer_intencion_humana(state.get("messages", []))
        logger = logging.getLogger("jarvi.agent")
        logger.info(f"[chatbot] contexto_tecnico recibido = {ctx}")
        logger.info(f"[chatbot] último mensaje: {ultimo_mensaje[:100] if ultimo_mensaje else ''}")

        # --- EXTRACCIÓN DE DATOS SIEMPRE (incluso en primera interacción) ---
        if ultimo_mensaje:
            try:
                extraccion = extractor_llm.invoke(
                    f"Del siguiente mensaje, extrae el nombre (campo 'nombre'), "
                    f"teléfono (campo 'telefono') y email (campo 'email'). "
                    f"Mensaje: {ultimo_mensaje}"
                )
                if extraccion.nombre:
                    ctx["nombre"] = extraccion.nombre
                if extraccion.telefono:
                    _, ctx["whatsapp"] = normalizar_contacto("", extraccion.telefono, "")
                if extraccion.email:
                    ctx["email"] = extraccion.email
            except Exception as e:
                logger.warning(f"Fallo en extracción LLM: {e}")

            # Ciudad manual
            if not ctx.get("ciudad"):
                ciudades = ["guatemala", "mixco", "capital", "villa nueva", "escuintla",
                            "jalapa", "chimaltenango", "chiquimula", "sacatepéquez",
                            "sololá", "quetzaltenango", "retalhuleu", "suchitepéquez"]
                for ciudad in ciudades:
                    if ciudad in ultimo_mensaje:
                        ctx["ciudad"] = ciudad.capitalize()
                        if ctx.get("requiere_auditoria_electrica"):
                            ctx["empresa_electrica"] = "EEGSA"
                            ctx["tarifa_base_gtq"] = 1.45
                        break

            # Topología manual
            if not ctx.get("topologia"):
                if any(k in ultimo_mensaje for k in ["red", "atado", "interconectado", "ahorro", "eegsa", "factura"]):
                    ctx["topologia"] = "On-Grid (Sistemas Atados a la Red)"
                    ctx["requiere_auditoria_electrica"] = True
                elif any(k in ultimo_mensaje for k in ["aislado", "batería", "bateria", "finca", "autónomo", "off-grid"]):
                    ctx["topologia"] = "Off-Grid (Sistemas Aislados)"
                    ctx["requiere_auditoria_electrica"] = True

        # --- ACTUALIZAR run_name CON EL WHATSAPP NORMALIZADO ---
        nombre_ctx = ctx.get("nombre", "Usuario")
        whatsapp_ctx = ctx.get("whatsapp", "Pendiente")
        nombre_run, whatsapp_run = normalizar_contacto(nombre_ctx, whatsapp_ctx, ctx.get("ciudad", ""))

        config["run_name"] = f"Lead: {nombre_run}"
        if "metadata" not in config:
            config["metadata"] = {}
        config["metadata"]["whatsapp"] = whatsapp_run
        config["metadata"]["topologia"] = ctx.get("topologia", "Desconocida")
        config["metadata"]["vendedor"] = ctx.get("vendedor", "gerencia@aisa.com.gt")

        # --- PRIMERA INTERACCIÓN: bienvenida guiada (con datos ya extraídos) ---
        if len(state.get("messages", [])) == 1:
            bienvenida = (
                "¡Hola! 👋 Soy Jarvi, tu asesor técnico de AISA Solar.\n"
                "Antes de comenzar, necesito algunos datos para poder ayudarte mejor:\n\n"
                "1️⃣ Tu nombre completo\n"
                "2️⃣ Tu número de WhatsApp (con código de país, ej. +502 1234-5678)\n"
                "3️⃣ Tu correo electrónico (para enviarte la cotización)\n"
                "4️⃣ ¿Qué productos te interesan? (ej. paneles solares, calentadores, bombas)\n\n"
                "¡Empecemos! ¿Cómo te llamas?"
            )
            logger.info(f"[chatbot] respuesta de bienvenida con datos extraídos: {ctx}")
            return {"messages": [AIMessage(content=bienvenida)], "contexto_tecnico": ctx}

        # --- PREGUNTAS GUIADAS PARA CAMPOS FALTANTES ---
        if not ctx.get("whatsapp") or ctx["whatsapp"] == "Pendiente":
            if not ctx.get("preguntando_whatsapp"):
                ctx["preguntando_whatsapp"] = True
                return {
                    "messages": [AIMessage(content="Para poder enviarte la cotización, necesito tu número de WhatsApp con código de país (ej. +502 1234-5678).")],
                    "contexto_tecnico": ctx
                }
        else:
            ctx["preguntando_whatsapp"] = False

        if not ctx.get("email"):
            if not ctx.get("preguntando_email"):
                ctx["preguntando_email"] = True
                return {
                    "messages": [AIMessage(content="¿A qué correo electrónico te gustaría que te enviemos la cotización?")],
                    "contexto_tecnico": ctx
                }
        else:
            ctx["preguntando_email"] = False

        if not ctx.get("productos") or len(ctx["productos"]) == 0:
            productos = buscar_productos_por_mensaje(ultimo_mensaje)
            if productos:
                ctx["productos"] = productos
            else:
                if not ctx.get("preguntando_productos"):
                    ctx["preguntando_productos"] = True
                    return {
                        "messages": [AIMessage(content="¿Qué tipo de sistemas solares te interesan? Por ejemplo: paneles solares, calentadores, bombas de agua, etc.")],
                        "contexto_tecnico": ctx
                    }
        else:
            ctx["preguntando_productos"] = False

        if not ctx.get("vendedor"):
            ctx["vendedor"] = asignar_vendedor(ctx.get("whatsapp"))

        if ctx.get("whatsapp") and ctx["whatsapp"] != "Pendiente":
            _, ctx["whatsapp"] = normalizar_contacto("", ctx["whatsapp"], "")

        # --- LOGS Y PERSISTENCIA ---
        logger.info(f"[chatbot] Extracción final: nombre={ctx.get('nombre')}, whatsapp={ctx.get('whatsapp')}, email={ctx.get('email')}")
        logger.info(f"[chatbot] Productos: {ctx.get('productos')}")
        logger.info(f"[chatbot] Vendedor: {ctx.get('vendedor')}")
        logger.info(f"[chatbot] Campos completos: {validar_campos_obligatorios(ctx)}")

        if validar_campos_obligatorios(ctx):
            try:
                import asyncio
                thread_id = config.get("configurable", {}).get("thread_id")
                if thread_id:
                    asyncio.run(actualizar_thread(
                        thread_id=thread_id,
                        nombre=ctx["nombre"],
                        whatsapp=ctx["whatsapp"],
                        email=ctx["email"],
                        productos=ctx["productos"],
                        vendedor=ctx["vendedor"],
                        trace_id=trace_id_var.get()
                    ))
                    asyncio.run(registrar_evento_auditoria(
                        thread_id=thread_id,
                        trace_id=trace_id_var.get(),
                        event_type="DATOS_CLIENTE_COMPLETOS",
                        source="chatbot_node",
                        payload=ctx
                    ))
                    logger.info(f"Datos del cliente persistidos: {ctx['nombre']} ({ctx['whatsapp']})")
            except Exception as e:
                logger.error(f"Error al persistir datos del cliente: {e}")

        # --- CONTINUAR CON EL FLUJO NORMAL DEL CHATBOT ---
        if ctx.get("requiere_auditoria_electrica"):
            regla_datos = "1. DEBES recopilar sutilmente: Nombre, Ubicación, Consumo y Necesidad exacta."
        else:
            regla_datos = "1. DEBES recopilar sutilmente: Nombre, Ubicación y Necesidad exacta."

        ontologia_dinamica = obtener_fragmento_ontologia(ctx.get("topologia"))

        prompt_sistema = SystemMessage(
            content=(
                f"Eres Jarvi, Ingeniero de Preventa de AISA Solar.\n"
                f"Responde con los datos auditados:\n"
                f"- Ubicación: {ctx.get('ciudad', 'PENDIENTE')}\n"
                f"- Distribuidora: {ctx.get('empresa_electrica', 'PENDIENTE')}\n"
                f"- Tarifa: GTQ {ctx.get('tarifa_base_gtq', 'PENDIENTE')} /kWh\n"
                f"REGLAS: {regla_datos}\n"
                f"ONTOLOGÍA: {ontologia_dinamica}\n"
                f"Cliente: {ctx.get('nombre', 'PENDIENTE')} | WhatsApp: {ctx.get('whatsapp', 'PENDIENTE')} | Email: {ctx.get('email', 'PENDIENTE')}\n"
                f"Productos de interés: {', '.join(ctx.get('productos', ['PENDIENTE']))}\n"
                f"Vendedor asignado: {ctx.get('vendedor', 'PENDIENTE')}"
            )
        )

        respuesta = llm.invoke([prompt_sistema] + state["messages"], config=config)
        if not respuesta.content:
            logger.warning("El LLM devolvió respuesta vacía. Usando fallback.")
            respuesta = AIMessage(
                content="Lo siento, no pude generar una respuesta en este momento. Por favor, intenta de nuevo."
            )

        logger.info(f"[chatbot] contexto después de LLM: {ctx}")
        return {"messages": [respuesta], "contexto_tecnico": ctx}

    # -------------------- Ensamblaje del grafo --------------------
    graph_builder.add_node("clasificador", clasificador_topologia_node)
    graph_builder.add_node("validador", validador_geolocalizacion_node)
    graph_builder.add_node("chatbot", chatbot_node)
    graph_builder.add_node("tools", ToolNode([procesar_oportunidad_backend]))

    graph_builder.add_edge(START, "clasificador")
    graph_builder.add_edge("clasificador", "validador")
    graph_builder.add_edge("validador", "chatbot")
    graph_builder.add_conditional_edges("chatbot", tools_condition)
    graph_builder.add_edge("tools", "chatbot")

    return graph_builder.compile(checkpointer=checkpointer)

