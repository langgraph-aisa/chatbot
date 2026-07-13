"""
ontology.py
Módulo de ontología del catálogo de productos de AISA Solar para JARVI 2.0.
Carga y filtra bloques de conocimiento desde un archivo JSON externo,
proporcionando fragmentos de contexto al motor de razonamiento.

Estándares aplicados:
- ISO/IEC/IEEE 12207:2008 (Ciclo de vida del software): este módulo es
  un elemento de configuración del sistema, diseñado para ser mantenido
  y verificado de forma independiente.
- ISO/IEC 26514:2021 (Documentación de software): todas las funciones están
  documentadas con descripciones, parámetros, valores de retorno y pruebas
  de caja negra.
- ISO/IEC 25010:2011 (Calidad del producto):
  * Adecuación funcional: selecciona las categorías de producto relevantes
    según la topología del sistema fotovoltaico.
  * Eficiencia de desempeño: utiliza un caché en memoria para evitar
    lecturas repetitivas del disco.
  * Fiabilidad: maneja rutas alternativas (ONTOLOGY_JSON_PATH) y errores
    de lectura sin interrumpir el servicio.
- ISO/IEC 29119:2022 (Pruebas de software - caja negra):
  Las pruebas sugeridas se describen en cada función.
"""

import os
import json
import logging
from typing import Optional, Dict, Any, List

# ---------------------------------------------------------------------------
# Configuración del Logger para observabilidad en entornos serverless
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Caché en memoria (Singleton) para evitar múltiples lecturas de disco
# ---------------------------------------------------------------------------
_ONTOLOGIA_CACHE: Optional[Dict[str, Any]] = None

# ---------------------------------------------------------------------------
# Resolución robusta de la ruta del archivo JSON
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FALLBACK_PATH = os.path.join(BASE_DIR, 'catalog_ontology.json')
DEFAULT_JSON_PATH = os.getenv("ONTOLOGY_JSON_PATH", FALLBACK_PATH)


def cargar_ontologia(file_path: str = DEFAULT_JSON_PATH) -> Dict[str, Any]:
    """
    Carga y cachea en memoria la taxonomía del catálogo de productos
    desde un archivo JSON externo. Implementa el patrón Singleton sobre
    la ejecución del contenedor para evitar lecturas repetitivas.

    Parámetros:
        file_path (str): ruta al archivo JSON de ontología. Por defecto
                         usa la variable de entorno ONTOLOGY_JSON_PATH o
                         el archivo 'catalog_ontology.json' en el mismo
                         directorio.

    Retorna:
        dict: diccionario con la ontología completa, donde cada clave
              es un identificador de categoría y el valor es otro
              diccionario con 'tag', 'nombre', 'url' y 'keywords'.

    Prueba de caja negra (ISO/IEC 29119):
        1. Primera llamada: debe leer el archivo y devolver el diccionario.
        2. Segunda llamada: debe devolver el mismo diccionario desde caché
           sin acceder al disco (verificar con logs).
        3. Archivo inexistente: debe lanzar FileNotFoundError.
        4. Archivo con JSON mal formado: debe lanzar json.JSONDecodeError
           y registrar un log crítico.
        5. Llamada concurrente: la caché es segura porque solo se asigna
           una vez (no se requiere lock adicional en este diseño simple,
           pero se puede probar con múltiples hilos para verificar que
           no hay doble carga).
    """
    global _ONTOLOGIA_CACHE
    if _ONTOLOGIA_CACHE is not None:
        return _ONTOLOGIA_CACHE

    if not os.path.exists(file_path):
        logger.error(f"Falta el activo crítico de taxonomía en la ruta: {file_path}")
        raise FileNotFoundError(f"Archivo de ontología no encontrado en {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            _ONTOLOGIA_CACHE = json.load(f)
        logger.info(
            f"Ontología cargada exitosamente en memoria desde {file_path}. "
            f"{len(_ONTOLOGIA_CACHE)} categorías indexadas."
        )
        return _ONTOLOGIA_CACHE
    except json.JSONDecodeError as je:
        logger.critical(f"Corrupción detectada en el esquema estructurado JSON: {str(je)}")
        raise
    except Exception as e:
        logger.critical(f"Fallo catastrófico en la lectura de ontología: {str(e)}")
        raise


def obtener_fragmento_ontologia(topologia: Optional[str]) -> str:
    """
    Realiza el ruteo epistemológico avanzado: selecciona los bloques de
    productos relevantes según la topología del sistema fotovoltaico
    detectada por el grafo del agente (On‑Grid, Off‑Grid, Bombeo, etc.).
    El resultado es una cadena de texto formateada que se inyecta en el
    prompt del LLM para ahorrar ventana de contexto (Context Window
    Preservation).

    Parámetros:
        topologia (str | None): topología detectada, puede ser None
                                o contener palabras como "ON-GRID",
                                "OFF-GRID", "BOMBA", etc.

    Retorna:
        str: fragmento de ontología con las categorías y enlaces
             correspondientes, listo para insertar en el system prompt.

    Prueba de caja negra (ISO/IEC 29119):
        1. topologia=None: devuelve bloques por defecto (11-15, 18, 20, 26, 38, 46, 51).
        2. topologia="ON-GRID": incluye los bloques 1-10, 20, 35, 37, 38, 60, 61, 64, 79-81, 85.
        3. topologia="OFF-GRID": incluye los bloques 14, 16, 18-20, 22-24, 26-28, 32, 34, 35, 45, 46, 50-53, 62, 64, 81, 82, 86.
        4. topologia="BOMBA SOLAR": incluye bloques de tuberías y bombas (12-16, 39-42, 55-59, 66, 73, 74, 77, 78, 83).
        5. Si falla la carga de ontología, devuelve el mensaje de error sin lanzar excepción.
        6. Si un bloque_id no existe en el catálogo, se omite y se registra un warning.
    """
    try:
        ontologia = cargar_ontologia()
    except Exception:
        return "Error interno de inicialización: Catálogo temporalmente indisponible."

    # Selección de bloques según topología
    if not topologia:
        bloques_requeridos = [
            "11", "12", "13", "14", "15", "18", "20",
            "26", "38", "46", "51"
        ]
    elif "ON-GRID" in topologia.upper() or "ATADO" in topologia.upper():
        bloques_requeridos = [
            str(i) for i in range(1, 11)
        ] + [
            "20", "35", "37", "38", "60", "61", "64",
            "79", "80", "81", "85"
        ]
    elif "OFF-GRID" in topologia.upper() or "AISLADO" in topologia.upper():
        bloques_requeridos = [
            "14", "16", "18", "19", "20", "22", "23",
            "24", "26", "27", "28", "32", "34", "35",
            "45", "46", "50", "51", "52", "53", "62",
            "64", "81", "82", "86"
        ]
    elif "BOMBA" in topologia.upper() or "HIDRO" in topologia.upper() or "BOMBEO" in topologia.upper():
        bloques_requeridos = [
            "12", "13", "15", "16", "39", "40", "41",
            "42", "55", "56", "57", "58", "59", "66",
            "73", "74", "77", "78", "83"
        ]
    else:
        bloques_requeridos = [
            "11", "12", "13", "14", "15", "18", "20",
            "26", "38", "46", "51"
        ]

    resultado: List[str] = []
    for bloque_id in bloques_requeridos:
        if bloque_id in ontologia:
            item = ontologia[bloque_id]
            resultado.append(
                f"Categoría [{bloque_id}]: {item['nombre']}\n"
                f"Enlace Directo: {item['url']}\n"
                f"Keywords de Validación: {', '.join(item['keywords'])}"
            )
        else:
            logger.warning(
                f"Se solicitó el bloque id '{bloque_id}', "
                f"pero no está definido en el catálogo JSON."
            )

    return "\n\n".join(resultado)


# =============================================================================
# Obtencion de productos estructurados para el flujo de preventa
# =============================================================================
def get_product_blocks(topologia: Optional[str], tipo: Optional[str] = None) -> List[str]:
    """
    Retorna los ids de bloques de productos por topologia y, opcionalmente,
    filtrados por tipo ("sistema" o "unitario").
    """
    if not topologia:
        base = ["11", "12", "13", "14", "15", "18", "20", "26", "38", "46", "51"]
    elif "ON-GRID" in topologia.upper() or "ATADO" in topologia.upper():
        base = [str(i) for i in range(1, 11)] + ["20", "35", "37", "38", "60", "61", "64", "79", "80", "81", "85"]
    elif "OFF-GRID" in topologia.upper() or "AISLADO" in topologia.upper():
        base = ["14", "16", "18", "19", "20", "22", "23", "24", "26", "27", "28", "32", "34", "35", "45", "46", "50", "51", "52", "53", "62", "64", "81", "82", "86"]
    elif "BOMBA" in topologia.upper() or "HIDRO" in topologia.upper() or "BOMBEO" in topologia.upper():
        base = ["12", "13", "15", "16", "39", "40", "41", "42", "55", "56", "57", "58", "59", "66", "73", "74", "77", "78", "83"]
    else:
        base = ["11", "12", "13", "14", "15", "18", "20", "26", "38", "46", "51"]

    if not tipo:
        return base

    ontologia = cargar_ontologia()
    return [
        bloque_id
        for bloque_id in base
        if bloque_id in ontologia and ontologia[bloque_id].get("tipo") == tipo
    ]


def obtener_productos_relevantes(
    topologia: str,
    tipo: Optional[str] = None,
    max_items: int = 5,
) -> List[Dict[str, Any]]:
    """
    Retorna hasta max_items productos del catalogo, con nombre, tag, url y tipo.
    """
    ontologia = cargar_ontologia()
    productos: List[Dict[str, Any]] = []
    for bloque_id in get_product_blocks(topologia, tipo)[:max_items]:
        item = ontologia.get(bloque_id)
        if not isinstance(item, dict):
            continue
        productos.append(
            {
                "nombre": item.get("nombre", ""),
                "tag": item.get("tag", ""),
                "url": item.get("url", ""),
                "tipo": item.get("tipo", "desconocido"),
            }
        )
    return productos


# =============================================================================
# NUEVA FUNCIÓN: Búsqueda de productos por mensaje (para recolección de datos)
# =============================================================================
def buscar_productos_por_mensaje(mensaje: str, top_n: int = 5) -> List[str]:
    """
    Busca en el catálogo los productos cuyas keywords coincidan con el mensaje del usuario.
    Retorna hasta top_n nombres de productos (según el campo 'nombre' de cada categoría).

    Esta función reutiliza el caché de cargar_ontologia(), por lo que es eficiente
    y consistente con el resto del sistema.

    Parámetros:
        mensaje (str): texto del usuario (en minúsculas o no).
        top_n (int): número máximo de productos a retornar.

    Retorna:
        List[str]: lista de nombres de productos encontrados.

    Prueba de caja negra (ISO/IEC 29119):
        1. Mensaje "quiero paneles solares y calentadores": debe devolver al menos
           los nombres que contengan "panel solar" y "calentador".
        2. Mensaje sin coincidencias: devuelve lista vacía.
        3. top_n=2: devuelve máximo 2 productos.
    """
    ontologia = cargar_ontologia()
    productos_encontrados = []
    mensaje_lower = mensaje.lower()
    for key, item in ontologia.items():
        if isinstance(item, dict) and "nombre" in item:
            palabras_clave = item.get("keywords", [])
            for keyword in palabras_clave:
                if keyword.lower() in mensaje_lower:
                    if item["nombre"] not in productos_encontrados:
                        productos_encontrados.append(item["nombre"])
                        if len(productos_encontrados) >= top_n:
                            return productos_encontrados
                    break
    return productos_encontrados
