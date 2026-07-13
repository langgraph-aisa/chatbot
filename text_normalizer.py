"""
text_normalizer.py
Módulo para normalización de texto, corrección ortográfica y fuzzy matching.
Usa symspellpy para corrección rápida, rapidfuzz para coincidencia difusa.
Con manejo de errores para evitar fallos en producción.
"""

import os
import re
import unicodedata
import logging
from typing import List, Optional, Dict, Any

from symspellpy import SymSpell, Verbosity
from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)

# =============================================================================
# Carga del diccionario de corrección ortográfica
# =============================================================================
SYMSPELL_PATH = os.getenv("SYMSPELL_DICT_PATH", "spanish_dictionary.txt")
_symspell = None

def obtener_symspell() -> Optional[SymSpell]:
    """Carga y cachea la instancia de SymSpell con el diccionario en español."""
    global _symspell
    if _symspell is None:
        try:
            _symspell = SymSpell(max_dictionary_edit_distance=2, prefix_length=7)
            if os.path.exists(SYMSPELL_PATH):
                _symspell.load_dictionary(SYMSPELL_PATH, term_index=0, count_index=1)
                logger.info(f"Diccionario SymSpell cargado desde {SYMSPELL_PATH}")
            else:
                logger.warning(f"No se encontró diccionario en {SYMSPELL_PATH}. Corrección ortográfica deshabilitada.")
                _symspell = None
        except Exception as e:
            logger.error(f"Error al cargar SymSpell: {e}")
            _symspell = None
    return _symspell

# =============================================================================
# Funciones de normalización
# =============================================================================
def normalizar_texto(texto: str) -> str:
    """
    Elimina tildes, convierte a minúsculas y elimina caracteres especiales.
    """
    if not texto:
        return ""
    texto = texto.lower()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    texto = re.sub(r'[^a-z0-9\s]', ' ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def corregir_ortografia(texto: str) -> str:
    """
    Corrige errores ortográficos usando SymSpell.
    """
    if not texto:
        return texto
    symspell = obtener_symspell()
    if symspell is None:
        return texto
    try:
        palabras = texto.split()
        corregidas = []
        for palabra in palabras:
            # Usar Verbosity.Closest o su equivalente numérico (1)
            # En algunas versiones, Verbosity.Closest puede no estar definido.
            # Usamos getattr para robustez.
            verbosity_closest = getattr(Verbosity, 'Closest', Verbosity.Top)
            suggestions = symspell.lookup(palabra, verbosity_closest, max_edit_distance=2)
            corregidas.append(suggestions[0].term if suggestions else palabra)
        return " ".join(corregidas)
    except Exception as e:
        logger.warning(f"Error en corrección ortográfica: {e}. Se devuelve texto original.")
        return texto

def normalizar_y_corregir(texto: str) -> str:
    """
    Aplica normalización y corrección ortográfica completa.
    """
    if not texto:
        return ""
    texto_norm = normalizar_texto(texto)
    return corregir_ortografia(texto_norm)

# =============================================================================
# Fuzzy matching para entidades
# =============================================================================
def buscar_coincidencia_fuzzy(texto: str, lista_referencia: List[str], umbral: float = 0.75) -> Optional[str]:
    """
    Busca la mejor coincidencia en una lista usando fuzzy matching.
    Retorna el elemento de la lista que mejor coincide, o None si no supera el umbral.
    """
    if not texto or not lista_referencia:
        return None
    texto_norm = normalizar_texto(texto)
    try:
        resultados = process.extract(texto_norm, lista_referencia, scorer=fuzz.token_set_ratio, limit=1)
        if resultados and resultados[0][1] >= umbral * 100:
            return resultados[0][0]
    except Exception as e:
        logger.warning(f"Error en fuzzy matching: {e}")
    return None

def buscar_entidad_en_json(texto: str, json_data: Dict[str, Any], key_lista: str = "nombre", aliases_key: str = "aliases", umbral: float = 0.75) -> Optional[Dict[str, Any]]:
    """
    Busca una entidad en un JSON usando fuzzy matching sobre el nombre y los aliases.
    """
    if not texto or not json_data:
        return None
    texto_norm = normalizar_texto(texto)
    mejor_item = None
    mejor_score = 0.0
    try:
        for item_id, item in json_data.items():
            if isinstance(item, dict):
                nombre = item.get(key_lista, "")
                if nombre:
                    score = fuzz.token_set_ratio(texto_norm, normalizar_texto(nombre)) / 100.0
                    if score > mejor_score:
                        mejor_score = score
                        mejor_item = item
                for alias in item.get(aliases_key, []):
                    if alias:
                        score = fuzz.token_set_ratio(texto_norm, normalizar_texto(alias)) / 100.0
                        if score > mejor_score:
                            mejor_score = score
                            mejor_item = item
        if mejor_item and mejor_score >= umbral:
            return mejor_item
    except Exception as e:
        logger.warning(f"Error en búsqueda de entidad en JSON: {e}")
    return None

def extraer_whatsapp_normalizado(mensaje: str) -> Optional[str]:
    """
    Extrae número de WhatsApp usando regex y lo normaliza.
    """
    if not mensaje:
        return None
    match = re.search(r"(\+?[0-9]{1,3}[-.\s]?)?[0-9][\s\-\.]?[0-9]{3,4}[\s\-\.]?[0-9]{3,4}", mensaje)
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

def extraer_nombre_con_regex(mensaje: str) -> Optional[str]:
    """
    Extrae nombre usando regex mejorado.
    """
    if not mensaje:
        return None
    patrones = [
        r"(?:me llamo|soy|mi nombre es|nombre:|llamo)\s*([A-Za-záéíóúñ\s]+)",
        r"([A-Za-záéíóúñ]{2,}\s+[A-Za-záéíóúñ]{2,})"
    ]
    for patron in patrones:
        match = re.search(patron, mensaje, re.IGNORECASE)
        if match:
            nombre = match.group(1).strip().title()
            if len(nombre.split()) >= 1:
                return nombre
    return None

# =============================================================================
# Pipeline completo (con captura de excepciones)
# =============================================================================
def pipeline_extraccion(mensaje: str, 
                        json_ubicacion: Dict[str, Any], 
                        json_productos: Dict[str, Any],
                        json_vendedores: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Pipeline completo de extracción de datos: normaliza, corrige, y extrae.
    Retorna un diccionario con los campos que pudo extraer.
    """
    resultado = {}
    if not mensaje:
        return resultado

    try:
        # Normalizar y corregir el mensaje
        mensaje_procesado = normalizar_y_corregir(mensaje)
        mensaje_original = mensaje.lower()

        # Extraer nombre
        nombre = extraer_nombre_con_regex(mensaje_procesado) or extraer_nombre_con_regex(mensaje)
        if nombre:
            resultado["nombre"] = nombre

        # Extraer WhatsApp
        whatsapp = extraer_whatsapp_normalizado(mensaje)
        if whatsapp:
            resultado["whatsapp"] = whatsapp

        # Extraer email
        email_match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", mensaje)
        if email_match:
            resultado["email"] = email_match.group(0)

        # Ubicación
        ubicacion_item = buscar_entidad_en_json(mensaje_procesado, json_ubicacion, key_lista="label", aliases_key="aliases", umbral=0.7)
        if ubicacion_item:
            resultado["ubicacion_label"] = ubicacion_item.get("label")
            resultado["municipio"] = ubicacion_item.get("municipio")
            resultado["departamento"] = ubicacion_item.get("departamento")

        # Productos
        lista_productos = []
        for item in json_productos.values():
            if isinstance(item, dict) and "nombre" in item:
                lista_productos.append(item["nombre"])
                lista_productos.extend(item.get("keywords", []))
        mejor_match = buscar_coincidencia_fuzzy(mensaje_procesado, lista_productos, umbral=0.7)
        if mejor_match:
            for item in json_productos.values():
                if isinstance(item, dict) and (item["nombre"] == mejor_match or mejor_match in item.get("keywords", [])):
                    resultado["productos"] = [item["nombre"]]
                    break

        # Vendedor
        vendedor_nombres = [v.get("nombre", "") for v in json_vendedores if v.get("nombre")]
        vendedor_match = buscar_coincidencia_fuzzy(mensaje_procesado, vendedor_nombres, umbral=0.7)
        if vendedor_match:
            for v in json_vendedores:
                if v.get("nombre") == vendedor_match:
                    resultado["vendedor"] = v.get("email")
                    break

        # Topología
        if any(k in mensaje_original for k in ["red", "atado", "interconectado", "ahorro", "eegsa", "factura"]):
            resultado["topologia"] = "On-Grid"
            resultado["requiere_auditoria_electrica"] = True
        elif any(k in mensaje_original for k in ["aislado", "batería", "bateria", "finca", "autónomo", "off-grid"]):
            resultado["topologia"] = "Off-Grid"
            resultado["requiere_auditoria_electrica"] = True

    except Exception as e:
        logger.error(f"Error en pipeline_extraccion: {e}", exc_info=True)
        # Retornamos lo que se haya podido extraer hasta el momento

    return resultado
