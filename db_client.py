"""
db_client.py
Cliente de base de datos para operaciones CRUD sobre threads, audit_events y telemetry_events.
Cumple con ISO/IEC 25010 (Fiabilidad) y 29119 (Pruebas).
"""

import os
import json
import asyncpg
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger("jarvi.db")

async def get_db_connection():
    """Obtiene una conexión a la base de datos PostgreSQL."""
    return await asyncpg.connect(os.getenv("DATABASE_URL"))

async def actualizar_thread(
    thread_id: str,
    nombre: str,
    whatsapp: str,
    email: Optional[str] = None,
    productos: Optional[List[str]] = None,
    vendedor: Optional[str] = None,
    trace_id: Optional[str] = None
) -> bool:
    """
    Inserta o actualiza la tabla threads con los datos del cliente.
    Retorna True si se actualizó correctamente, False en caso contrario.
    """
    conn = None
    try:
        conn = await get_db_connection()
        # Normalizar WhatsApp
        from agent_graph import normalizar_contacto
        _, whatsapp_norm = normalizar_contacto("", whatsapp, "")

        # Verificar si existe por whatsapp_id
        existing = await conn.fetchrow(
            "SELECT thread_id, metadata FROM threads WHERE whatsapp_id = $1",
            whatsapp_norm
        )

        metadata = {
            "email": email,
            "productos": productos or [],
            "vendedor": vendedor,
            "trace_id": trace_id
        }

        if existing:
            old_meta = existing["metadata"] or {}
            if isinstance(old_meta, str):
                try:
                    old_meta = json.loads(old_meta)
                except json.JSONDecodeError:
                    old_meta = {}
            if "cumulative_cost" in old_meta:
                metadata["cumulative_cost"] = old_meta["cumulative_cost"]
            # Actualizar
            await conn.execute(
                """
                UPDATE threads
                SET nombre_cliente = $1, metadata = $2
                WHERE whatsapp_id = $3
                """,
                nombre,
                json.dumps(metadata),
                whatsapp_norm
            )
            logger.info(f"Thread actualizado: {thread_id} - {nombre} ({whatsapp_norm})")
        else:
            metadata["cumulative_cost"] = 0.0
            # Insertar
            await conn.execute(
                """
                INSERT INTO threads (thread_id, nombre_cliente, whatsapp_id, metadata)
                VALUES ($1, $2, $3, $4)
                """,
                thread_id,
                nombre,
                whatsapp_norm,
                json.dumps(metadata)
            )
            logger.info(f"Nuevo thread creado: {thread_id} - {nombre} ({whatsapp_norm})")
        return True
    except Exception as e:
        logger.error(f"Error al actualizar thread: {e}")
        return False
    finally:
        if conn:
            await conn.close()

async def obtener_thread_por_whatsapp(whatsapp: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene los datos del cliente a partir de su número de WhatsApp normalizado.
    """
    conn = None
    try:
        conn = await get_db_connection()
        from agent_graph import normalizar_contacto
        _, whatsapp_norm = normalizar_contacto("", whatsapp, "")
        row = await conn.fetchrow(
            "SELECT thread_id, nombre_cliente, whatsapp_id, metadata FROM threads WHERE whatsapp_id = $1",
            whatsapp_norm
        )
        if row:
            return {
                "thread_id": row["thread_id"],
                "nombre": row["nombre_cliente"],
                "whatsapp": row["whatsapp_id"],
                "metadata": row["metadata"]
            }
        return None
    except Exception as e:
        logger.error(f"Error al obtener thread: {e}")
        return None
    finally:
        if conn:
            await conn.close()

async def registrar_evento_auditoria(
    thread_id: str,
    trace_id: str,
    event_type: str,
    source: str,
    payload: Dict[str, Any],
    langsmith_run_id: Optional[str] = None
) -> bool:
    """
    Inserta un evento en la tabla audit_events.
    """
    conn = None
    try:
        conn = await get_db_connection()
        await conn.execute(
            """
            INSERT INTO audit_events (
                thread_id, timestamp, event_type, source,
                system_snapshot, request_payload, langsmith_run_id
            )
            VALUES ($1, NOW(), $2, $3, $4, $5, $6)
            """,
            thread_id,
            event_type,
            source,
            json.dumps({"trace_id": trace_id}),
            json.dumps(payload),
            langsmith_run_id
        )
        logger.info(f"Evento de auditoría registrado: {trace_id} - {event_type}")
        return True
    except Exception as e:
        logger.error(f"Error al registrar evento de auditoría: {e}")
        return False
    finally:
        if conn:
            await conn.close()


async def acumular_costo_thread(thread_id: str, costo: float) -> bool:
    """
    Acumula costo en metadata.cumulative_cost para un thread existente.
    """
    conn = None
    try:
        conn = await get_db_connection()
        await conn.execute(
            """
            UPDATE threads
            SET metadata = jsonb_set(
                metadata,
                '{cumulative_cost}',
                to_jsonb(COALESCE((metadata->>'cumulative_cost')::numeric, 0) + $1)
            )
            WHERE thread_id = $2
            """,
            costo,
            thread_id,
        )
        return True
    except Exception as e:
        logger.error(f"Error al acumular costo: {e}")
        return False
    finally:
        if conn:
            await conn.close()


async def obtener_costo_acumulado(thread_id: str) -> float:
    """
    Obtiene metadata.cumulative_cost para un thread.
    """
    conn = None
    try:
        conn = await get_db_connection()
        row = await conn.fetchrow(
            "SELECT metadata->>'cumulative_cost' as cost FROM threads WHERE thread_id = $1",
            thread_id,
        )
        return float(row["cost"]) if row and row["cost"] else 0.0
    except Exception as e:
        logger.error(f"Error al obtener costo acumulado: {e}")
        return 0.0
    finally:
        if conn:
            await conn.close()
