from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time
import json

from .schemas import CheckRequest, CheckResponse
from .pipeline import OptimizedOnkoPipeline
from .config import logger

app = FastAPI(
    title="OncoAssistant ML Service",
    version="2.0",
    description="Optimized ML service for oncology treatment verification with deterministic validation"
)

# CORS для фронта
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Инициализация пайплайна при старте
pipeline = OptimizedOnkoPipeline()


@app.middleware("http")
async def add_timing_header(request: Request, call_next):
    """Middleware для добавления времени обработки в заголовки"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


@app.post("/check", response_model=CheckResponse)
async def check_treatment(request: CheckRequest):
    """
    Основной эндпоинт проверки лечения.

    Поддерживает:
    - Двухуровневую валидацию (детерминированную + LLM)
    - Фильтрацию по метаданным
    - Оптимизированный реранкер с fallback
    """
    logger.info(f"Received request {request.request_id} for role {request.role}")

    try:
        response = await pipeline.run(request)
        return response
    except Exception as e:
        logger.exception(f"Error processing request {request.request_id}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/check/batch")
async def check_treatment_batch(requests: list[CheckRequest]):
    """
    Batch processing для нескольких пациентов.
    Полезно для тестирования golden cases.
    """
    results = []
    errors = []

    for request in requests:
        try:
            result = await pipeline.run(request)
            results.append(result)
        except Exception as e:
            logger.error(f"Batch error for {request.request_id}: {e}")
            errors.append({"request_id": request.request_id, "error": str(e)})

    return {
        "processed": len(results),
        "failed": len(errors),
        "results": results,
        "errors": errors
    }


@app.get("/health")
async def health():
    """
    Проверка работоспособности сервиса.
    Возвращает статус всех компонентов.
    """
    health_status = {
        "status": "ok",
        "timestamp": time.time(),
        "components": {}
    }

    try:
        # Проверка retriever
        from .retrieval import OptimizedGuidelineRetriever
        retriever = OptimizedGuidelineRetriever()
        health_status["components"]["retriever"] = "loaded"

        # Проверка реранкера
        from .models import OptimizedRerankerModel
        reranker = OptimizedRerankerModel()
        health_status["components"]["reranker"] = "loaded"

        # Проверка валидатора
        from .validators import TherapyValidator
        validator = TherapyValidator()
        health_status["components"]["validator"] = "loaded"

        # Проверка конфигурации
        health_status["configuration"] = {
            "use_reranker": pipeline.use_reranker,
            "reranker_candidates": pipeline.reranker_candidates,
            "use_metadata_filter": pipeline.use_metadata_filter,
            "use_query_expansion": pipeline.use_query_expansion,
            "has_gpu": pipeline.has_gpu
        }

    except Exception as e:
        health_status["status"] = "degraded"
        health_status["error"] = str(e)

    return health_status


@app.get("/stats")
async def get_stats():
    """
    Статистика работы сервиса (для мониторинга).
    """
    return {
        "cache_hits": pipeline.retriever._cache_hits,
        "avg_processing_time": "calculated_from_logs",
        "configuration": {
            "use_reranker": pipeline.use_reranker,
            "reranker_candidates": pipeline.reranker_candidates,
            "use_metadata_filter": pipeline.use_metadata_filter,
            "use_query_expansion": pipeline.use_query_expansion,
        }
    }


@app.get("/")
async def root():
    return {
        "service": "OnkoAssistant ML Service",
        "version": "2.0",
        "features": [
            "hybrid_retrieval",
            "metadata_filtering",
            "deterministic_validation",
            "llm_explanation",
            "query_expansion"
        ]
    }


# Обработчики ошибок
@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )

