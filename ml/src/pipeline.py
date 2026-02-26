import time
import json
from typing import List, Optional, Dict, Any
from datetime import datetime

from .retrieval import OptimizedGuidelineRetriever
from .models import OptimizedRerankerModel
from .generator import generate_response
from .query_builder import build_query_from_input
from .validators import TherapyValidator, ComorbidityChecker
from .schemas import CheckRequest, CheckResponse, Result, Issue, Source, InputData
from .config import logger


class OptimizedOnkoPipeline:
    """
    Оптимизированный пайплайн с:
    - Двухуровневой валидацией (детерминированная + LLM)
    - Умным fallback для реранкера
    - Фильтрацией по метаданным
    - Структурированным логированием метрик
    """

    def __init__(self):
        self.retriever = OptimizedGuidelineRetriever()
        self.reranker = OptimizedRerankerModel(use_onnx=False)  # ONNX опционально
        self.validator = TherapyValidator()
        self.comorbidity_checker = ComorbidityChecker()

        # Конфигурация производительности
        self.use_reranker = True
        self.reranker_candidates = 10  # уменьшено с 20 для скорости
        self.reranker_top_k = 3
        self.use_metadata_filter = True
        self.use_query_expansion = True

        # Проверка доступности GPU
        import torch
        self.has_gpu = torch.cuda.is_available()
        if not self.has_gpu:
            logger.warning("GPU not available. Reranker will use CPU (consider reducing candidates)")
            # Автоматически уменьшаем число кандидатов на CPU
            self.reranker_candidates = 8

        logger.info(f"Pipeline initialized: GPU={self.has_gpu}, reranker_candidates={self.reranker_candidates}")

    async def run(self, request: CheckRequest) -> CheckResponse:
        """Основной метод обработки запроса"""
        start_time = time.time()
        request_id = request.request_id

        # Инициализация метрик
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "request_id": request_id,
            "nosology": request.input.diagnosis.icd10,
            "stage": request.input.diagnosis.stage,
            "therapy_line": request.input.treatment.therapy_line,
            "regimen": request.input.treatment.proposed_regimen,
            "role": request.role.value,
        }

        logger.info(f"Processing request {request_id} for {request.input.diagnosis.name}, stage {request.input.diagnosis.stage}")

        try:
            t1 = time.time()
            query_text = build_query_from_input(request.input, use_expansion=self.use_query_expansion)
            metrics["query_build_time"] = time.time() - t1
            metrics["query_length"] = len(query_text)
            logger.debug(f"Built query: {query_text[:200]}...")

            metadata_filters = None
            if self.use_metadata_filter:
                metadata_filters = self._build_nosology_filter(request.input)
                logger.debug(f"Metadata filters: {metadata_filters}")

            t2 = time.time()
            retrieved = self.retriever.search(
                query_text,
                metadata_filters=metadata_filters,
                top_k=self.reranker_candidates if self.use_reranker else 3
            )
            retrieval_time = time.time() - t2
            metrics["retrieval_time"] = retrieval_time
            metrics["retrieved_count"] = len(retrieved)

            if not retrieved:
                logger.warning(f"No documents found for request {request_id}")
                metrics["error"] = "no_documents_found"
                self._log_metrics(metrics, time.time() - start_time)
                return self._build_empty_response(request, "Не найдено релевантных клинических рекомендаций")

            t3 = time.time()
            reranked = self._rerank_with_fallback(query_text, retrieved)
            rerank_time = time.time() - t3
            metrics["rerank_time"] = rerank_time
            metrics["reranked_count"] = len(reranked)

            t4 = time.time()
            deterministic_issues = self.validator.validate(request.input, reranked)
            comorbidity_issues = self.comorbidity_checker.check_organ_dysfunction(request.input)
            all_deterministic_issues = deterministic_issues + comorbidity_issues
            validation_time = time.time() - t4
            metrics["validation_time"] = validation_time
            metrics["deterministic_issues_count"] = len(all_deterministic_issues)

            # Классификация severity
            critical_count = sum(1 for i in all_deterministic_issues if i.severity == "critical")
            high_count = sum(1 for i in all_deterministic_issues if i.severity == "high")
            metrics["critical_issues"] = critical_count
            metrics["high_issues"] = high_count

            t5 = time.time()

            # Если есть критические проблемы, можем сразу сформировать ответ
            # но лучше дать LLM возможность их объяснить
            llm_result = generate_response(
                query_text=query_text,
                retrieved_chunks=reranked,
                role=request.role.value,
                locale=request.locale.value,
                request_id=request_id,
                deterministic_issues=all_deterministic_issues  # передаем для контекста
            )
            llm_time = time.time() - t5
            metrics["llm_time"] = llm_time

            t6 = time.time()
            response = self._build_response(request, llm_result, reranked, all_deterministic_issues)
            build_time = time.time() - t6

            # Итоговые метрики
            total_time = time.time() - start_time
            metrics["build_response_time"] = build_time
            metrics["total_time"] = total_time
            metrics["is_compliant"] = response.result.is_compliant
            metrics["final_issues_count"] = len(response.result.issues)

            # Проверка бюджета времени (10 секунд)
            if total_time > 10:
                logger.warning(f"Request {request_id} exceeded time budget: {total_time:.2f}s")
                metrics["time_budget_exceeded"] = True

            self._log_metrics(metrics, total_time)

            logger.info(f"Request {request_id} completed in {total_time:.2f}s, compliant={response.result.is_compliant}")
            return response

        except Exception as e:
            logger.exception(f"Error processing request {request_id}: {str(e)}")
            metrics["error"] = str(e)
            metrics["error_type"] = type(e).__name__
            self._log_metrics(metrics, time.time() - start_time)
            return self._build_error_response(request, str(e))

    def _build_metadata_filters(self, input_data: InputData) -> Optional[Dict[str, Any]]:
        """Формирует фильтры по метаданным из входных данных"""
        filters = {}

        # Нозология (ICD-10 или название)
        if input_data.diagnosis.icd10:
            filters["nosology"] = input_data.diagnosis.icd10
        elif input_data.diagnosis.name:
            # Нормализация названия
            name_map = {
                "рак легкого": "C34",
                "рак молочной железы": "C50",
                "немелкоклеточный рак легкого": "C34-NSCLC",
            }
            filters["nosology"] = name_map.get(input_data.diagnosis.name.lower(), input_data.diagnosis.name)

        # Стадия
        if input_data.diagnosis.stage:
            filters["stage"] = input_data.diagnosis.stage.upper().replace("СТАДИЯ", "").strip()

        # Линия терапии
        if input_data.treatment.therapy_line:
            filters["therapy_line"] = input_data.treatment.therapy_line

        # Гистология
        if input_data.diagnosis.histology:
            histology_map = {
                "аденокарцинома": "adenocarcinoma",
                "сквамозный": "squamous",
                "мелкоклеточный": "SCLC",
            }
            for key, value in histology_map.items():
                if key in input_data.diagnosis.histology.lower():
                    filters["histology"] = value
                    break

        return filters if filters else None

    def _build_nosology_filter(self, input_data: InputData) -> Optional[Dict]:
        """
        УМНЫЙ фильтр специально для EGJ / пищеводно-желудочного перехода.
        """
        if not input_data.diagnosis.name:
            return None

        original = input_data.diagnosis.name.lower().strip()

        # Если это явно EGJ — расширяем максимально широко
        if any(x in original for x in ["пищеводно-желудоч", "эзофагогастраль", "egj", "кардио", "esophagogastric"]):
            # Используем только $eq операторы, так как Chroma не поддерживает $contains для строк
            return {
                "$or": [
                    {"nosology": {"$eq": "рак желудка"}},
                    {"nosology": {"$eq": "рак пищевода"}},
                    {"nosology": {"$eq": "аденокарцинома пищевода и пищеводно-желудочного перехода"}},
                    {"nosology": {"$eq": "рак пищеводно-желудочного перехода"}},
                    {"nosology": {"$eq": "esophagogastric junction cancer"}},
                    {"nosology": {"$eq": "egj adenocarcinoma"}},
                ]
            }

        # Для CUP — вообще не фильтруем (или отдельная логика)
        if "невыявленного первичного очага" in original or "cup" in original.lower():
            return None

        # Для остальных — обычный фильтр
        return {"nosology": {"$eq": original}}

    def _rerank_with_fallback(self, query_text: str, retrieved: List) -> List:
        """
        Умный реранкинг с оценкой времени и fallback.
        Если реранкер медленный — используем только гибридный поиск.
        """
        if not self.use_reranker or len(retrieved) <= 3:
            # Реранкер не нужен, возвращаем топ-3 от гибридного поиска
            return retrieved[:3]

        # Оценка времени реранкера
        estimated_time = self.reranker.estimate_time(min(len(retrieved), self.reranker_candidates))

        if estimated_time > 2.0 and not self.has_gpu:
            # Слишком долго, пропускаем реранкер
            logger.warning(f"Reranker estimated time {estimated_time:.2f}s exceeds threshold, using hybrid only")
            return retrieved[:3]

        # Реранкер на топ-N кандидатах
        candidates = retrieved[:self.reranker_candidates]
        docs = [r[0] for r in candidates]

        try:
            indices = self.reranker.rerank(
                query_text,
                docs,
                top_k=self.reranker_top_k,
                return_indices=True,
                batch_size=4  # меньшие батчи для скорости
            )
            reranked = [candidates[i] for i in indices]
            return reranked
        except Exception as e:
            logger.error(f"Reranker failed: {e}, falling back to hybrid ranking")
            return retrieved[:3]

    def _build_response(self, request: CheckRequest, llm_result: dict,
                       retrieved_chunks: list, deterministic_issues: List[Issue]) -> CheckResponse:
        """Формирует финальный ответ, объединяя LLM и детерминированные проверки"""

        # Объединяем issues от LLM и детерминированные
        llm_issues = llm_result.get("issues", [])
        all_issues = deterministic_issues.copy()

        # Добавляем issues от LLM, которых нет в детерминированных по коду
        existing_codes = {i.code for i in all_issues}
        for iss in llm_issues:
            if iss.get("code") not in existing_codes:
                all_issues.append(Issue(**iss))

        # Сортируем по severity
        severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        all_issues.sort(key=lambda x: severity_order.get(x.severity, 99))

        # Определяем is_compliant
        is_compliant = len(all_issues) == 0 or all(i.severity not in ["critical", "high"] for i in all_issues)

        # Формируем sources
        sources = []
        if request.options.return_sources:
            for src in llm_result.get("_sources", []):
                sources.append(Source(**src))

        result_obj = Result(
            is_compliant=is_compliant,
            summary=llm_result.get("summary", ""),
            issues=all_issues,
            doctor_explanation=llm_result.get("doctor_explanation", ""),
            patient_explanation=llm_result.get("patient_explanation", ""),
            recommended_next_steps=llm_result.get("recommended_next_steps", [])
        )

        return CheckResponse(
            request_id=request.request_id,
            status="ok",
            result=result_obj,
            sources=sources,
            warnings=[]
        )

    def _build_empty_response(self, request: CheckRequest, message: str) -> CheckResponse:
        """Ответ при отсутствии документов"""
        return CheckResponse(
            request_id=request.request_id,
            status="warning",
            result=Result(
                is_compliant=False,
                summary=message,
                issues=[],
                doctor_explanation=message,
                patient_explanation="К сожалению, не удалось найти подходящие рекомендации для проверки вашего случая.",
                recommended_next_steps=["Обратитесь к специалисту для ручной проверки"]
            ),
            sources=[],
            warnings=["no_relevant_guidelines_found"]
        )

    def _build_error_response(self, request: CheckRequest, error_msg: str) -> CheckResponse:
        """Ответ при ошибке"""
        return CheckResponse(
            request_id=request.request_id,
            status="error",
            result=Result(
                is_compliant=False,
                summary="Ошибка обработки",
                issues=[],
                doctor_explanation=f"Произошла техническая ошибка: {error_msg}",
                patient_explanation="Извините, произошла ошибка. Пожалуйста, попробуйте позже.",
                recommended_next_steps=[]
            ),
            sources=[],
            warnings=[f"error: {error_msg}"]
        )

    def _log_metrics(self, metrics: Dict, total_time: float):
        """Структурированное логирование метрик"""
        # JSON для машинной обработки
        logger.info(f"METRICS_JSON: {json.dumps(metrics, ensure_ascii=False, default=str)}")

        # Читаемый формат для человека
        perf_summary = (
            f"Performance: total={total_time:.2f}s, "
            f"retrieval={metrics.get('retrieval_time', 0):.3f}s, "
            f"rerank={metrics.get('rerank_time', 0):.3f}s, "
            f"llm={metrics.get('llm_time', 0):.2f}s, "
            f"issues={metrics.get('final_issues_count', 0)}"
        )
        logger.info(perf_summary)
