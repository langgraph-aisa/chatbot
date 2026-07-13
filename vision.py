"""
vision.py
Módulo de procesamiento de imágenes para JARVI 2.0.
Utiliza el modelo GPT‑4o‑mini con visión para extraer datos estructurados
de facturas eléctricas de Guatemala (empresa, consumo, monto).
Este módulo está diseñado para ser ejecutado en el backend (API central).

Estándares aplicados:
- ISO/IEC/IEEE 12207:2008 (Ciclo de vida): módulo reutilizable de
  procesamiento de imágenes.
- ISO/IEC 26514:2021 (Documentación): todas las funciones incluyen
  descripciones, parámetros, retornos y pruebas de caja negra.
- ISO/IEC 25010:2011 (Calidad del producto):
  * Funcionalidad: extrae exactamente los campos requeridos.
  * Fiabilidad: maneja la ausencia de la API key y respuestas malformadas.
- ISO/IEC 29119:2022 (Pruebas de software - caja negra):
  Las pruebas sugeridas se documentan en la función.
"""

import base64
import json
import os
import requests
from openai import OpenAI

# ---------------------------------------------------------------------------
# Nota: El cliente OpenAI se instancia de forma perezosa dentro de la
# función para evitar fallos si la API key no está configurada al importar
# el módulo. En su lugar, se lanza una excepción controlada si se intenta
# procesar sin la clave.
# ---------------------------------------------------------------------------

def _obtener_cliente_openai() -> OpenAI:
    """
    Crea y retorna una instancia del cliente OpenAI utilizando la clave
    desde la variable de entorno OPENAI_API_KEY.

    Lanza:
        RuntimeError: si OPENAI_API_KEY no está definida.

    Prueba de caja negra:
        - Con OPENAI_API_KEY definida: devuelve una instancia de OpenAI.
        - Sin la variable: lanza RuntimeError.
    """
    api_key = os.getenv("OPENAI_API_KEY_1") or os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY_2")
    if not api_key:
        raise RuntimeError(
            "No se puede procesar la imagen: no hay API key de OpenAI configurada."
        )
    return OpenAI(api_key=api_key)


def descargar_imagen_desde_url(url: str) -> bytes:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content


def procesar_imagen_factura(base64_image: str) -> dict:
    """
    Analiza una imagen de factura eléctrica de Guatemala (en formato Base64)
    y extrae datos estructurados: empresa eléctrica, consumo en kWh y monto
    en quetzales.

    Parámetros:
        base64_image (str): cadena con la imagen codificada en Base64
                            (sin el prefijo 'data:image/...;base64,').

    Retorna:
        dict: diccionario con las claves 'empresa_electrica', 'consumo_kwh',
              'monto_factura'. Si no se detecta un dato, su valor será None.

    Lanza:
        RuntimeError: si la API key de OpenAI no está configurada.
        openai.OpenAIError: si la llamada a la API falla.
        json.JSONDecodeError: si la respuesta de la API no es un JSON válido.
        KeyError: si la respuesta JSON no contiene la estructura esperada.

    Prueba de caja negra (ISO/IEC 29119):
        1. Imagen válida de EEGSA con datos claros:
           - 'empresa_electrica' debe ser "EEGSA" (o similar).
           - 'consumo_kwh' debe ser un número.
           - 'monto_factura' debe ser un número.
        2. Imagen sin datos (papel en blanco):
           - Todos los campos deben ser None.
        3. base64_image vacío o inválido: debe lanzarse una excepción
           (dependiendo de la API, puede ser un error de OpenAI).
        4. Verificar que no se haga más de una llamada innecesaria a OpenAI
           (no se implementa caché en este módulo, pero la API central
           podría añadirlo).
        5. Con OPENAI_API_KEY no definida: RuntimeError antes de llamar
           a la API.
    """
    # Obtener cliente OpenAI (lanza RuntimeError si no hay API key)
    client = _obtener_cliente_openai()

    # Llamada al modelo de visión
    respuesta = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Analiza esta factura de electricidad de Guatemala. "
                            "Extrae en formato JSON estricto: "
                            "1. 'empresa_electrica' (Busca EEGSA o ENERGUATE), "
                            "2. 'consumo_kwh' (sólo el número), "
                            "3. 'monto_factura' (sólo el número en Quetzales). "
                            "Si no detectas un dato, asigna null."
                        )
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{base64_image}"
                        }
                    }
                ]
            }
        ],
        response_format={"type": "json_object"}
    )

    # La respuesta viene como un string JSON; lo parseamos
    contenido = respuesta.choices[0].message.content
    datos = json.loads(contenido)

    # Normalizar salida: asegurar que las tres claves existan
    resultado = {
        "empresa_electrica": datos.get("empresa_electrica", None),
        "consumo_kwh": datos.get("consumo_kwh", None),
        "monto_factura": datos.get("monto_factura", None)
    }
    return resultado


def procesar_imagen_desde_url(url: str) -> dict:
    image_bytes = descargar_imagen_desde_url(url)
    base64_image = base64.b64encode(image_bytes).decode("utf-8")
    return procesar_imagen_factura(base64_image)
