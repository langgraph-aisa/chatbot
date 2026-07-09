import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from models.schemas import ChatRequest
from services.audio_service import AudioTranscriptionError
from services.chat_service import ChatService


router = APIRouter()
logger = logging.getLogger("jarvi.api")


def get_chat_service(request: Request) -> ChatService:
    service = getattr(request.app.state, "chat_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="Grafo no inicializado")
    return service


@router.post("/chat")
async def chat_endpoint(
    payload: ChatRequest,
    chat_service: ChatService = Depends(get_chat_service),
):
    try:
        message = await chat_service.resolver_mensaje_entrada(
            payload.message,
            payload.url_n8n_audio,
        )
    except AudioTranscriptionError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if not message:
        logger.warning(
            "Payload /chat rechazado sin contenido: has_message=%s, has_audio_url=%s, has_phone=%s",
            bool(payload.message),
            bool(payload.url_n8n_audio),
            bool(payload.phone),
        )
        raise HTTPException(
            status_code=400,
            detail={
                "error": "sin_contenido",
                "message": "Enviar message/text/body con texto o url_n8n_audio/audio_url con una URL de audio.",
            },
        )

    thread_id = (payload.thread_id or payload.phone or payload.name or "n8n-default-thread").strip()

    logger.info(
        "Payload /chat normalizado: thread_id=%s, name=%s, phone=%s, record_items=%s",
        thread_id,
        payload.name,
        payload.phone,
        len(payload.record),
    )

    return StreamingResponse(
        chat_service.generar_tokens(thread_id, message, payload.record),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )
