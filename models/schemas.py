"""
schemas.py
Contratos de datos (Modelos Pydantic) para la API central de JARVI 2.0.
Define las estructuras de solicitud y respuesta que garantizan la
interoperabilidad entre los canales (Streamlit, n8n, LangSmith) y el backend.

Estándares aplicados:
- ISO/IEC/IEEE 12207:2008 (Ciclo de vida del software): los modelos son
  artefactos de diseño que forman parte de la especificación de la API.
- ISO/IEC 26514:2021 (Documentación de software): cada modelo y campo
  incluye descripciones que facilitan su comprensión y uso.
- ISO/IEC 25010:2011 (Calidad del producto):
  * Adecuación funcional: los campos reflejan exactamente la información
    necesaria para el proceso de preventa.
  * Usabilidad: las descripciones (`description`) sirven como documentación
    automática en Swagger UI.
- ISO/IEC 29119:2022 (Pruebas de software - caja negra):
  Las pruebas sugeridas se incluyen en cada clase.
"""

import json

from pydantic import BaseModel, Field, field_validator
from typing import Dict, Any, Optional


class ChatRequest(BaseModel):
    """
    Modelo de solicitud para el endpoint de chat (/chat).
    Representa un mensaje enviado por cualquier canal (humano o máquina)
    hacia el agente conversacional.

    Prueba de caja negra (ISO/IEC 29119):
        1. Enviar un JSON válido con `thread_id` y `message` → 200 OK.
        2. Omitir `thread_id` → error de validación (422 Unprocessable Entity).
        3. Omitir `message` → error de validación (422).
        4. Incluir `metadata` vacío → debe ser aceptado.
        5. Incluir `metadata` con campos adicionales (ej. `"source": "n8n"`)
           → debe ser aceptado sin errores.
    """
    thread_id: str = Field(
        default="",
        description="ID único de sesión del cliente. Permite mantener el "
                    "estado conversacional y la persistencia en PostgreSQL."
    )
    message: str = Field(
        default="",
        description="Contenido del mensaje del cliente. Puede ser texto "
                    "plano, pregunta técnica o descripción de necesidad."
    )
    url_n8n_audio: str = Field(
        default="",
        description="URL temporal enviada por n8n cuando el mensaje llega como audio. "
                    "Si viene vacia, se usa el campo message directamente."
    )
    name: str = Field(
        default="",
        description="Nombre del usuario o cliente que envía el mensaje. "
                    "Opcional, pero útil para personalizar la respuesta."
    )
    phone: str = Field(
        default="",
        description="Número de teléfono del cliente. Opcional, pero útil "
                    "para canales que requieren contacto directo."
    )
    record: str = Field(
        default="",
        description="Historial de la conversación en formato JSON. Se utiliza para "
                    "mantener el contexto de la conversación."
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Metadatos adicionales inyectados por el canal "
                    "(fuente, tags, etc.) que se propagan al sistema de "
                    "trazabilidad (LangSmith, auditoría)."
    )


    @field_validator("thread_id", "message", "url_n8n_audio", "name", "phone", "record", mode="before")
    @classmethod
    def normalizar_campos_texto(cls, value):
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)


class ChatResponse(BaseModel):
    """
    Modelo de respuesta del endpoint de chat (en modo no streaming).
    Contiene la respuesta completa del agente y el identificador de
    ejecución para la auditoría.

    Prueba de caja negra (ISO/IEC 29119):
        1. Después de una solicitud exitosa, el JSON de respuesta debe
           contener los campos `response`, `run_id` y `status`.
        2. `status` debe ser exactamente "success" en condiciones normales.
        3. `run_id` debe ser un string no vacío.
        4. `response` debe contener la respuesta textual del agente.
    """
    response: str = Field(
        ...,
        description="Texto completo de la respuesta del agente."
    )
    run_id: str = Field(
        ...,
        description="Identificador único de la ejecución del grafo, "
                    "utilizado para la trazabilidad en LangSmith y "
                    "la auditoría forense (tabla audit_events)."
    )
    status: str = Field(
        default="success",
        description="Estado de la operación. En caso de éxito siempre "
                    "es 'success'."
    )


# ---------------------------------------------------------------------------
# Modelos adicionales para endpoints auxiliares (futuro)
# ---------------------------------------------------------------------------
class AudioRequest(BaseModel):
    """
    Modelo para la solicitud de transcripción (Speech‑to‑Text).
    Pendiente de implementación completa en la API.
    """
    thread_id: str = Field(..., description="ID de sesión asociado al audio.")
    # Se espera que el audio se envíe como multipart/form-data; este modelo
    # es una representación conceptual.


class ImageRequest(BaseModel):
    """
    Modelo para la solicitud de análisis de factura (visión artificial).
    Pendiente de implementación completa en la API.
    """
    thread_id: str = Field(..., description="ID de sesión asociado a la imagen.")
    image_base64: str = Field(..., description="Imagen codificada en Base64.")


class TTSRequest(BaseModel):
    """
    Modelo para la solicitud de síntesis de voz (Text‑to‑Speech).
    Pendiente de implementación completa en la API.
    """
    text: str = Field(..., description="Texto a convertir en voz.")
    voice: str = Field(default="alloy", description="Voz del modelo TTS.")
