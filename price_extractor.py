"""
price_extractor.py - Extracción de precios desde URLs con caché y manejo de errores.
VERSIÓN 1.2 – Integración con ontología y logs detallados de fuente.
17AGO2026.
"""
import time
import re
import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, List
from urllib.parse import urlparse

logger = logging.getLogger(__name__)
CACHE_TTL = 3600  # 1 hora
_cache = {}
_cache_time = {}


def extract_price_from_url(url: str) -> Optional[Dict[str, any]]:
    """
    Extrae el precio (en GTQ) desde la URL del producto.
    Retorna {'precio': float, 'moneda': str, 'fuente': str} o None.
    """
    if not url:
        logger.warning("[PRICE-EXTRACTOR] ⚠️ URL vacía, no se puede extraer precio")
        return None

    # Verificar caché
    now = time.time()
    if url in _cache and (now - _cache_time.get(url, 0)) < CACHE_TTL:
        logger.info(f"[PRICE-EXTRACTOR] 💾 Precio obtenido de caché para {url}")
        return _cache[url]

    logger.info(f"[PRICE-EXTRACTOR] 🔍 Iniciando extracción desde URL: {url}")

    # Registrar dominio para trazabilidad
    parsed_url = urlparse(url)
    dominio = parsed_url.netloc
    logger.info(f"[PRICE-EXTRACTOR] 📍 Dominio: {dominio}")

    try:
        # Descargar la página con timeout
        resp = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        resp.raise_for_status()
        logger.info(f"[PRICE-EXTRACTOR] ✅ Página descargada (status: {resp.status_code}, tamaño: {len(resp.text)} bytes)")

        # Parsear HTML
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Buscar precio - Selectores comunes en tiendas WooCommerce
        selectors = [
            '.price .amount',
            '.product-price .amount',
            '.woocommerce-Price-amount',
            '.price span.woocommerce-Price-amount',
            '.entry-summary .price .amount',
            '.product-info .price',
            '.product .price .amount',
            'span.woocommerce-Price-amount',
            '.summary .price .amount'
        ]

        price_elem = None
        selector_usado = None
        for selector in selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                selector_usado = selector
                logger.info(f"[PRICE-EXTRACTOR] ✅ Selector encontrado: {selector}")
                break

        if not price_elem:
            # Último intento: buscar cualquier elemento con precio
            price_elem = soup.find(class_=re.compile(r'price'))
            if price_elem:
                price_elem = price_elem.find(class_=re.compile(r'amount'))
                if price_elem:
                    selector_usado = "price.amount (fallback)"
                    logger.info(f"[PRICE-EXTRACTOR] ✅ Selector fallback encontrado: {selector_usado}")

        if not price_elem:
            logger.warning(f"[PRICE-EXTRACTOR] ❌ No se encontró elemento de precio en {url}")
            return None

        # Extraer texto del precio
        price_text = price_elem.get_text(strip=True)
        logger.info(f"[PRICE-EXTRACTOR] 📝 Texto de precio extraído: '{price_text}'")

        # Detectar moneda
        moneda = "GTQ"
        simbolo_moneda = "Q"
        if "$" in price_text or "USD" in price_text:
            moneda = "USD"
            simbolo_moneda = "$"
            logger.info(f"[PRICE-EXTRACTOR] 💱 Moneda detectada: USD")
        elif "Q" in price_text or "GTQ" in price_text:
            moneda = "GTQ"
            simbolo_moneda = "Q"
            logger.info(f"[PRICE-EXTRACTOR] 💱 Moneda detectada: GTQ")
        elif "€" in price_text:
            moneda = "EUR"
            simbolo_moneda = "€"
            logger.info(f"[PRICE-EXTRACTOR] 💱 Moneda detectada: EUR")

        # Limpiar precio: eliminar símbolos de moneda, comas, espacios
        price_clean = re.sub(r'[^\d.,]', '', price_text)
        price_clean = price_clean.replace(',', '')
        price_clean = price_clean.strip()

        if not price_clean:
            logger.warning(f"[PRICE-EXTRACTOR] ❌ No se encontraron dígitos en el precio: '{price_text}'")
            return None

        try:
            price = float(price_clean)
            result = {
                "precio": price,
                "moneda": moneda,
                "simbolo": simbolo_moneda,
                "fuente": url,
                "selector": selector_usado,
                "texto_original": price_text
            }
            _cache[url] = result
            _cache_time[url] = now
            logger.info(f"[PRICE-EXTRACTOR] ✅ Precio extraído exitosamente: {simbolo_moneda} {price:,.2f} desde {url}")
            logger.info(f"[PRICE-EXTRACTOR] 📋 Detalles: Moneda={moneda}, Selector={selector_usado}")
            return result
        except ValueError as e:
            logger.error(f"[PRICE-EXTRACTOR] ❌ No se pudo convertir a número: '{price_clean}' - Error: {e}")
            return None

    except requests.exceptions.Timeout:
        logger.error(f"[PRICE-EXTRACTOR] ❌ Timeout al extraer precio de {url}")
    except requests.exceptions.RequestException as e:
        logger.error(f"[PRICE-EXTRACTOR] ❌ Error HTTP al extraer precio de {url}: {e}")
    except Exception as e:
        logger.error(f"[PRICE-EXTRACTOR] ❌ Error inesperado al extraer precio de {url}: {e}")

    return None


def extract_prices_from_urls(urls: list) -> dict:
    """Extrae precios de múltiples URLs y retorna un diccionario {url: result}."""
    results = {}
    for url in urls:
        if url:
            results[url] = extract_price_from_url(url)
            if not results[url]:
                logger.warning(f"[PRICE-EXTRACTOR] ⚠️ No se pudo extraer precio de {url}")
    return results
