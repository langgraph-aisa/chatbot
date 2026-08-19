"""
odoo_db_client.py - Cliente asíncrono para consultar la base de datos PostgreSQL de Odoo.
VERSIÓN 1.6 – Búsqueda flexible en name y description_sale.
17AGO2026.
"""
import os
import logging
import asyncio
from typing import List, Dict, Optional, Any
import asyncpg
from config import settings

logger = logging.getLogger(__name__)

class OdooDBClient:
    """Cliente para consultar product_template directamente desde PostgreSQL."""

    def __init__(self):
        # Lectura directa de variables de entorno (con fallback a settings)
        self.host = os.getenv("DATABASE_HOST") or settings.odoo_db_host
        self.port = int(os.getenv("DATABASE_PORT") or settings.odoo_db_port or 5432)
        self.database = os.getenv("DATABASE_PROD") or settings.odoo_db_name
        self.user = os.getenv("DATABASE_USER") or settings.odoo_db_user
        self.password = os.getenv("DATABASE_PASSWORD") or settings.odoo_db_password
        self.pool: Optional[asyncpg.Pool] = None
        self._initialized = False
        self._connection_attempts = 0
        self._max_retries = 3
        self._last_error: Optional[str] = None
        self._connection_history: List[Dict] = []

        logger.info(f"[ODOO-DB] Configuración: host={self.host}, port={self.port}, database={self.database}, user={self.user}")

    async def connect(self) -> bool:
        """Inicializa el pool de conexiones a la BD de Odoo con 3 reintentos."""
        if self._initialized:
            return True

        self._connection_attempts = 0
        self._connection_history = []

        while self._connection_attempts < self._max_retries:
            self._connection_attempts += 1
            attempt_info = {
                "attempt": self._connection_attempts,
                "timestamp": str(asyncio.get_event_loop().time()),
                "host": self.host,
                "port": self.port,
                "database": self.database
            }

            logger.info(f"[ODOO-DB] 🔄 Intento de conexión #{self._connection_attempts} a {self.host}:{self.port}/{self.database}")

            try:
                self.pool = await asyncpg.create_pool(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    min_size=1,
                    max_size=5,
                    timeout=10.0
                )

                async with self.pool.acquire() as conn:
                    result = await conn.fetchval("SELECT 1")
                    if result == 1:
                        self._initialized = True
                        self._last_error = None
                        attempt_info["status"] = "success"
                        self._connection_history.append(attempt_info)
                        logger.info(f"[ODOO-DB] ✅ Conexión establecida correctamente (intento #{self._connection_attempts})")
                        return True
                    else:
                        raise Exception("Falló la consulta de verificación")

            except Exception as e:
                self._initialized = False
                self._last_error = str(e)
                attempt_info["status"] = "failed"
                attempt_info["error"] = str(e)
                self._connection_history.append(attempt_info)
                logger.error(f"[ODOO-DB] ❌ Error en conexión #{self._connection_attempts}: {self._last_error}")

                if self._connection_attempts < self._max_retries:
                    wait_time = 3 * self._connection_attempts
                    logger.info(f"[ODOO-DB] ⏳ Reintentando en {wait_time} segundos...")
                    await asyncio.sleep(wait_time)

        logger.error(f"[ODOO-DB] ❌ Máximo de reintentos alcanzado ({self._max_retries}). Todas las conexiones fallaron.")
        return False

    async def close(self):
        if self.pool:
            await self.pool.close()
            self._initialized = False
            logger.info("[ODOO-DB] Conexión cerrada.")

    async def ensure_connected(self) -> bool:
        """Asegura que la conexión esté activa, reconecta si es necesario."""
        if self._initialized:
            try:
                async with self.pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                    return True
            except Exception as e:
                logger.warning(f"[ODOO-DB] Conexión perdida: {e}. Reconectando...")
                self._initialized = False
                return await self.connect()
        return await self.connect()

    async def search_products_flexible(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Busca productos cuyo nombre o descripción de venta contengan la keyword (ILIKE).
        Retorna hasta 'limit' resultados con campos clave.
        """
        if not await self.ensure_connected():
            logger.warning("[ODOO-DB] ⚠️ No se pudo establecer conexión, retornando lista vacía")
            return []

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT id, name, list_price, description_sale
                    FROM product_template
                    WHERE name ILIKE '%' || $1 || '%'
                       OR description_sale ILIKE '%' || $1 || '%'
                    ORDER BY 
                        CASE WHEN name ILIKE '%' || $1 || '%' THEN 1 ELSE 2 END,
                        LENGTH(name)
                    LIMIT $2
                """, keyword, limit)
                results = [dict(r) for r in rows]
                if results:
                    logger.info(f"[ODOO-DB] ✅ Búsqueda flexible por '{keyword}': {len(results)} resultados")
                    for r in results:
                        logger.info(f"[ODOO-DB]    - {r.get('name')} - Q {r.get('list_price')}")
                else:
                    logger.info(f"[ODOO-DB] ℹ️ Búsqueda flexible por '{keyword}': 0 resultados")
                return results
        except Exception as e:
            logger.error(f"[ODOO-DB] ❌ Error en búsqueda flexible: {e}")
            self._initialized = False
            return []

    async def get_product_by_id(self, product_id: int) -> Optional[Dict[str, Any]]:
        """Obtiene un producto por su ID."""
        if not await self.ensure_connected():
            return None
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT id, name, list_price, description_sale
                    FROM product_template
                    WHERE id = $1
                """, product_id)
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"[ODOO-DB] Error al obtener producto por ID: {e}")
            return None

    async def get_products_by_category(self, categ_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        """Obtiene productos de una categoría específica."""
        if not await self.ensure_connected():
            return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT id, name, list_price, description_sale
                    FROM product_template
                    WHERE categ_id = $1
                    LIMIT $2
                """, categ_id, limit)
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"[ODOO-DB] Error en búsqueda por categoría: {e}")
            return []

    async def get_bom_components(self, product_tmpl_id: int) -> List[Dict[str, Any]]:
        """Retorna los componentes de la lista de materiales (mrp_bom)."""
        if not await self.ensure_connected():
            return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT bom.product_id, bom.product_qty, pt.name, pt.list_price
                    FROM mrp_bom bom
                    JOIN product_template pt ON bom.product_id = pt.id
                    WHERE bom.product_tmpl_id = $1
                """, product_tmpl_id)
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"[ODOO-DB] Error al obtener BOM: {e}")
            return []

    def format_price(self, price: Optional[float]) -> str:
        """Formatea el precio en Quetzales (Q) con dos decimales."""
        if price is None:
            return "Precio bajo consulta"
        try:
            return f"Q {price:,.2f}".replace(",", ".")
        except:
            return "Precio bajo consulta"

    def get_connection_status(self) -> dict:
        return {
            "connected": self._initialized,
            "attempts": self._connection_attempts,
            "max_retries": self._max_retries,
            "last_error": self._last_error,
            "history": self._connection_history,
            "host": self.host,
            "port": self.port,
            "database": self.database
        }

# Instancia global
odoo_db_client = OdooDBClient()
