"""
debug_app.py
Consola de depuración CLI para JARVI 2.0.
Proxy SSE entre el frontend (HTML) y el backend (FastAPI).
Mantiene la misma lógica de recolección de datos (nombre, WhatsApp, email, productos, vendedor)
al reenviar las peticiones al backend, que es el encargado de ejecutar el grafo y persistir.
Cumple con ISO/IEC 25010 (Fiabilidad) y 29119 (Pruebas).
"""

import os
import json
import uuid
import logging
from flask import Flask, render_template, request, Response, jsonify, stream_with_context
import requests
from urllib.parse import urljoin

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
BACKEND_URL = os.getenv("BACKEND_URL", "https://jarvi-backend-production.up.railway.app").rstrip('/')
API_KEY = os.getenv("CHATBOT_MASTER_API_KEY")
DEBUG_PORT = int(os.getenv("PORT", 8080))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("debug-console")

if not API_KEY:
    logger.error("CHATBOT_MASTER_API_KEY no definida. Las peticiones fallarán.")

app = Flask(__name__)


def backend_headers(extra: dict | None = None) -> dict:
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    if extra:
        headers.update({k: v for k, v in extra.items() if v})
    return headers

# ---------------------------------------------------------------------------
# Ruta principal: sirve la interfaz CLI
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('debug.html', backend_url=BACKEND_URL)

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.route('/health')
def health():
    return jsonify({"status": "ok", "service": "cliente-debug", "backend": BACKEND_URL})

# ---------------------------------------------------------------------------
# Proxy SSE: recibe mensaje del frontend y lo reenvía al backend
# ---------------------------------------------------------------------------
@app.route('/api/chat/stream', methods=['POST'])
def chat_stream():
    """
    Recibe el mensaje del usuario y el thread_id (si no se envía, se genera uno).
    Reenvía la petición al backend y retransmite el SSE tal cual.
    """
    data = request.get_json()
    if not data or 'message' not in data:
        return jsonify({"error": "Mensaje requerido"}), 400

    thread_id = data.get('thread_id', str(uuid.uuid4()))
    message = data['message']
    metadata = dict(data.get("metadata") or {})
    fingerprint = request.headers.get("X-Fingerprint") or metadata.get("fingerprint")
    chat_id = request.headers.get("X-Chat-ID") or metadata.get("chat_id")
    if fingerprint:
        metadata["fingerprint"] = fingerprint
    if chat_id:
        metadata["chat_id"] = chat_id
    logger.info(f"Debug request | thread: {thread_id} | msg: {message[:50]}...")

    def generate():
        # Evento de inicio
        yield f"data: {json.dumps({'info': f'[DEBUG] Conectando a {BACKEND_URL}'})}\n\n"

        headers = backend_headers({"X-Fingerprint": fingerprint, "X-Chat-ID": chat_id})
        payload = dict(data)
        payload.update({"thread_id": thread_id, "message": message, "metadata": metadata})

        try:
            resp = requests.post(
                urljoin(BACKEND_URL, '/chat'),
                json=payload,
                headers=headers,
                stream=True,
                timeout=120
            )

            if resp.status_code != 200:
                yield f"data: {json.dumps({'error': f'HTTP {resp.status_code}: {resp.text[:200]}'})}\n\n"
                return

            # Retransmitir cada línea del SSE sin modificaciones
            for line in resp.iter_lines(decode_unicode=True):
                if line:
                    yield f"data: {line}\n\n"

        except requests.exceptions.ConnectionError:
            yield f"data: {json.dumps({'error': '[ERROR] No se pudo conectar al backend'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': f'[ERROR] {str(e)}'})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


@app.route('/api/vision/analyze', methods=['POST'])
def analyze_image_proxy():
    data = request.get_json(silent=True) or {}
    try:
        resp = requests.post(
            urljoin(BACKEND_URL, '/api/vision/analyze'),
            json=data,
            headers=backend_headers(),
            timeout=120,
        )
        return Response(resp.content, status=resp.status_code, content_type=resp.headers.get("Content-Type", "application/json"))
    except Exception as e:
        logger.error(f"Error proxy vision: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/stt', methods=['POST'])
def speech_to_text_proxy():
    data = request.get_json(silent=True) or {}
    try:
        resp = requests.post(
            urljoin(BACKEND_URL, '/api/stt'),
            json=data,
            headers=backend_headers(),
            timeout=120,
        )
        return Response(resp.content, status=resp.status_code, content_type=resp.headers.get("Content-Type", "application/json"))
    except Exception as e:
        logger.error(f"Error proxy STT: {e}")
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------------
# Endpoint de historial (para futura implementación)
# ---------------------------------------------------------------------------
@app.route('/api/history/<thread_id>', methods=['GET'])
def get_history(thread_id):
    """Devuelve el historial de mensajes (mock)."""
    return jsonify({"thread_id": thread_id, "messages": []})

# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=DEBUG_PORT, debug=False)
