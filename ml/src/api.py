from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time
import json
from typing import Dict

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


# src/api.py (добавить)

from .schemas import ChatRequest, ChatResponse, ChatMessage


@app.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    """
    Диалоговый режим для уточняющих вопросов.
    Сохраняет роль (doctor/patient) из основного запроса.
    """
    from .gigachat_client import get_gigachat
    client = get_gigachat()

    # Системный промпт в зависимости от роли
    if request.role == "doctor":
        system = """Ты — эксперт-онколог, помогаешь коллеге интерпретировать результаты проверки лечения.

В начале всегда подтверждай, что коллега действует профессионально:
"Коллега, вы основываете лечение на самых передовых международных и российских клинических рекомендациях. Пациенту назначено лучшее лечение, которое сегодня предлагает современная медицина."

Затем переходи к анализу:
- Если всё хорошо: подтверди правильность выбора, отметь сильные стороны назначения.
- Если есть недочёты: указывай на них конструктивно, без обвинений. Используй формулировки: "Обратите внимание на...", "Возможно, стоит рассмотреть...", "Рекомендуется уточнить...".
- Никаких резких формулировок, слов "ошибка", "неправильно", "нарушение". Только профессиональные рекомендации.

Тон: уважительный, поддерживающий, коллегиальный."""

    else:  # patient
        system = """Ты — доброжелательный помощник пациента. Отвечаешь на вопросы о лечении.

В начале всегда подчёркивай, что врач действует в интересах пациента и назначил лучшее лечение:
"Ваш врач основывает лечение на самых передовых международных и российских клинических рекомендациях. Вам назначено лучшее лечение, которое сегодня предлагает современная медицина."

Правила общения:
- Объясняй простым языком, без сложных медицинских терминов.
- Если пациент тревожится — успокаивай, снижай напряжение.
- Если есть вопросы по лечению — объясняй мягко, но всегда направляй к врачу за деталями.
- Никогда не говори об ошибках врача. Только: "Ваш врач может уточнить...", "Для ещё большей эффективности...".
- Вселяй уверенность: "Вы в надёжных руках", "Команда врачей рядом с вами".

Тон: добрый, заботливый, успокаивающий."""

    # Формируем историю
    messages = [{"role": "system", "content": system}]

    # Если есть контекст (данные пациента), добавляем как первое сообщение
    if request.context:
        context_str = json.dumps(request.context, ensure_ascii=False)
        messages.append({
            "role": "user",
            "content": f"Контекст пациента: {context_str}"
        })

    # Добавляем историю диалога
    for msg in request.history:
        messages.append({"role": msg.role, "content": msg.content})

    # Добавляем новый вопрос
    messages.append({"role": "user", "content": request.message})

    # Вызов GigaChat
    response = client.chat_completion(messages)
    reply = response["choices"][0]["message"]["content"]

    # Обновляем историю
    new_history = request.history + [
        ChatMessage(role="user", content=request.message),
        ChatMessage(role="assistant", content=reply)
    ]

    return ChatResponse(
        request_id=request.request_id,
        message=reply,
        history=new_history
    )