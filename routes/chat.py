from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from models.schemas import ChatRequest
from services.audio_service import AudioTranscriptionError
from services.chat_service import ChatService


router = APIRouter()


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
        raise HTTPException(status_code=400, detail="message vacio y url_n8n_audio vacio")

    print(f"Nombre del campo recibido: {payload.name} tipo: {type(payload.name)}")
    print(f"Teléfono del campo recibido: {payload.phone} tipo: {type(payload.phone)}")
    print(f"Historial del campo recibido: {payload.record} tipo: {type(payload.record)}")

    return StreamingResponse(
        chat_service.generar_tokens(payload.thread_id, message),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )
