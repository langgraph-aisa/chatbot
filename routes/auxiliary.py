from fastapi import APIRouter, HTTPException

from schemas import AudioRequest, ImageRequest, TTSRequest


router = APIRouter()


@router.post("/ack/{trace_id}")
async def acknowledge_dispatch(trace_id: str):
    return {"status": "ACK received", "trace_id": trace_id}


@router.post("/stt")
async def speech_to_text(request: AudioRequest):
    raise HTTPException(status_code=501, detail="No implementado - pendiente centralizar Whisper")


@router.post("/tts")
async def text_to_speech(request: TTSRequest):
    raise HTTPException(status_code=501, detail="No implementado - pendiente centralizar TTS")


@router.post("/vision/analyze")
async def analizar_factura(request: ImageRequest):
    raise HTTPException(status_code=501, detail="No implementado - pendiente centralizar vision")


@router.post("/products")
async def consultar_productos(topologia: str = "on-grid"):
    raise HTTPException(status_code=501, detail="No implementado - pendiente exponer catalogo")
