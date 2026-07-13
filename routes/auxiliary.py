import asyncio

from fastapi import APIRouter, HTTPException

from models.schemas import AudioRequest, ImageRequest, TTSRequest
from ontology import obtener_productos_relevantes
from services.audio_service import AudioTranscriptionError, AudioTranscriptionService
from vision import procesar_imagen_desde_url, procesar_imagen_factura


router = APIRouter()


@router.post("/ack/{trace_id}")
async def acknowledge_dispatch(trace_id: str):
    return {"status": "ACK received", "trace_id": trace_id}


@router.post("/stt")
@router.post("/api/stt")
async def speech_to_text(request: AudioRequest):
    if not request.url:
        raise HTTPException(status_code=400, detail="Enviar url con el audio a transcribir")
    try:
        transcript = await AudioTranscriptionService().transcribe_audio_url(request.url)
    except AudioTranscriptionError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"transcript": transcript, "thread_id": request.thread_id}


@router.post("/tts")
async def text_to_speech(request: TTSRequest):
    raise HTTPException(status_code=501, detail="No implementado - pendiente centralizar TTS")


@router.post("/vision/analyze")
@router.post("/api/vision/analyze")
async def analizar_factura(request: ImageRequest):
    try:
        if request.url:
            extracted_data = await asyncio.to_thread(procesar_imagen_desde_url, request.url)
        elif request.image_base64:
            extracted_data = await asyncio.to_thread(procesar_imagen_factura, request.image_base64)
        else:
            raise HTTPException(status_code=400, detail="Enviar url o image_base64")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"No se pudo analizar la imagen: {exc}") from exc
    return {"extracted_data": extracted_data, "thread_id": request.thread_id}


@router.post("/products")
@router.post("/api/products")
async def consultar_productos(topologia: str = "on-grid"):
    return {"products": obtener_productos_relevantes(topologia=topologia, max_items=5)}
