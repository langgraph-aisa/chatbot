#!/bin/bash
# setup_redis.sh
# Script para verificar conexión a Redis y configurar parámetros de sesión.

REDIS_URL=${REDIS_URL:-"redis://localhost:6379/0"}

echo "=== Configurando Redis para JARVI 2.0 ==="
echo "Conectando a $REDIS_URL"

# Verificar que redis-cli esté instalado
if ! command -v redis-cli &> /dev/null; then
    echo "Error: redis-cli no encontrado. Instale redis-tools o utilice el cliente de su preferencia."
    exit 1
fi

# Conectar a Redis y ejecutar comandos
redis-cli -u "$REDIS_URL" <<EOF
# Limpiar sesiones antiguas (opcional, comentar si no se desea)
# KEYS session:* | xargs redis-cli DEL

# Configurar TTL por defecto para nuevas claves (no es directamente configurable, se hace al insertar)
# Pero podemos mostrar ejemplos.

# Mostrar información de la base de datos
INFO stats
INFO memory

# Verificar que no haya sesiones acumuladas
DBSIZE

# Ejemplo de inserción de una sesión de prueba (comentado)
# HSET session:test thread_id "test-123" whatsapp "+502 1234-5678" nombre "Prueba"
# EXPIRE session:test 604800

# Mostrar estructura de una sesión de ejemplo
HGETALL session:test 2>/dev/null || echo "No hay sesiones de prueba."
EOF

echo "=== Redis configurado correctamente ==="
echo "Recuerde establecer REDIS_URL en su entorno."
