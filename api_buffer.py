# api_buffer.py
"""
Módulo de buffer de sesión para JARVI 2.0.
Mantiene la asociación cliente ↔ thread_id en Redis,
almacena el estado de la conversación y coordina la persistencia final.
"""

import os
import json
import uuid
import asyncio
import logging
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

import redis.asyncio as redis

logger = logging.getLogger("jarvi.buffer")

# -------------------------------
# Modelo de Estado de Sesión
# -------------------------------
class SessionState(BaseModel):
    """Estado completo de la sesión del cliente."""
    thread_id: str
    whatsapp: str = ""
    nombre: str = "Usuario"
    contexto_tecnico: Dict[str, Any] = Field(default_factory=dict)
    pasos_completados: list = Field(default_factory=list)
    fase_actual: str = "inicio"
    ultimo_mensaje: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)

# -------------------------------
# Clase Principal del Buffer
# -------------------------------
class APIBuffer:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL")
        if not self.redis_url:
            logger.warning("REDIS_URL no configurada; APIBuffer queda inactivo hasta configurarse")
        self.ttl = int(os.getenv("REDIS_TTL", 604800))  # 7 días
        self._redis = None
        self._lock = asyncio.Lock()

    async def _get_redis(self) -> redis.Redis:
        """Obtiene la conexión a Redis (lazy singleton)."""
        if not self.redis_url:
            raise RuntimeError("REDIS_URL no configurada")
        if self._redis is None:
            async with self._lock:
                if self._redis is None:
                    self._redis = redis.from_url(
                        self.redis_url,
                        decode_responses=True,
                        max_connections=10
                    )
                    logger.info("Conexión a Redis establecida")
        return self._redis

    def _build_key(self, identifier: str) -> str:
        """Genera la clave para Redis."""
        return f"session:{identifier}"

    async def get_or_create_session(
        self,
        identifier: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> SessionState:
        """
        Obtiene una sesión existente por identificador (whatsapp/número)
        o crea una nueva si no existe.
        """
        redis_client = await self._get_redis()
        key = self._build_key(identifier)

        data = await redis_client.get(key)
        if data:
            try:
                state = SessionState(**json.loads(data))
                logger.debug(f"Sesión recuperada: {identifier} -> {state.thread_id}")
                # Actualizar metadatos si se proporcionan nuevos
                if metadata:
                    state.metadata.update(metadata)
                return state
            except Exception as e:
                logger.error(f"Error al deserializar sesión: {e}")

        # Crear nueva sesión
        new_thread_id = str(uuid.uuid4())
        new_state = SessionState(
            thread_id=new_thread_id,
            whatsapp=identifier if self._is_whatsapp(identifier) else "",
            nombre=metadata.get("nombre", "Usuario") if metadata else "Usuario",
            contexto_tecnico={},
            metadata=metadata or {}
        )

        # Guardar en Redis
        await redis_client.setex(
            key,
            self.ttl,
            new_state.json()
        )
        logger.info(f"Nueva sesión creada: {identifier} -> {new_thread_id}")
        return new_state

    async def update_session(self, state: SessionState) -> bool:
        """
        Actualiza el estado de la sesión en Redis.
        Si el whatsapp cambió, actualiza también la clave.
        """
        redis_client = await self._get_redis()
        old_identifier = self._extract_identifier(state)
        new_identifier = state.whatsapp or old_identifier

        # Si el identificador cambió (por ejemplo, se obtuvo el número),
        # eliminamos la clave antigua y creamos la nueva.
        if new_identifier != old_identifier and old_identifier:
            old_key = self._build_key(old_identifier)
            await redis_client.delete(old_key)
            logger.debug(f"Clave antigua eliminada: {old_key}")

        key = self._build_key(new_identifier)
        await redis_client.setex(key, self.ttl, state.json())
        logger.debug(f"Sesión actualizada: {key}")
        return True

    async def finalize_session(self, state: SessionState) -> bool:
        """
        Transfiere los datos del buffer a PostgreSQL (persistencia final)
        y elimina la clave de Redis.
        """
        # Aquí se podría invocar un worker o una función asíncrona
        # que inserte en threads, audit_events, telemetry_events, etc.
        # Por ahora, solo registramos la acción y borramos la clave.
        redis_client = await self._get_redis()
        key = self._build_key(state.whatsapp or self._extract_identifier(state))
        await redis_client.delete(key)
        logger.info(f"Sesión finalizada y eliminada: {key}")
        # Llamar a función de persistencia (mock)
        await self._persist_to_postgres(state)
        return True

    async def _persist_to_postgres(self, state: SessionState):
        """
        (Mock) En una implementación real, aquí se insertan los datos
        en las tablas de PostgreSQL (threads, audit_events, etc.).
        """
        logger.info(f"Persistiendo thread_id {state.thread_id} en PostgreSQL")
        # Ejemplo de inserción (usando asyncpg o SQLAlchemy)
        # ...

    def _extract_identifier(self, state: SessionState) -> str:
        """Extrae el identificador principal de la sesión."""
        return state.whatsapp or state.metadata.get("whatsapp") or state.metadata.get("number") or ""

    def _is_whatsapp(self, value: str) -> bool:
        """Verifica si el valor parece un número de WhatsApp (simple)."""
        return len(value) >= 8 and value.isdigit()

# -------------------------------
# Instancia global (singleton)
# -------------------------------
api_buffer = APIBuffer()
