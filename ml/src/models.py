import torch
import os
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from .config import MODELS_CACHE_DIR, logger


class EmbeddingModel:
    """Singleton для модели эмбеддингов"""
    _instance = None

    def __new__(cls, model_name="intfloat/multilingual-e5-large-instruct"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.model_name = model_name
            cls._instance.device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"Loading embedding model {model_name} on {cls._instance.device}")
            cls._instance.model = SentenceTransformer(
                model_name, cache_folder=MODELS_CACHE_DIR
            ).to(cls._instance.device)
            cls._instance.task = "Given a clinical query, retrieve relevant treatment guidelines"
        return cls._instance

    def encode_queries(self, texts):
        """Для поисковых запросов добавляем инструкцию"""
        instructed = [f"Instruct: {self.task}\\nQuery: {text}" for text in texts]
        return self.model.encode(instructed, normalize_embeddings=True)

    def encode_documents(self, texts):
        return self.model.encode(texts, normalize_embeddings=True)


class OptimizedRerankerModel:
    """
    Оптимизированный реранкер с:
    - Поддержкой ONNX Runtime (3-5x ускорение на CPU)
    - Fallback на отключение при необходимости
    - Батчевой обработкой для снижения overhead
    """
    _instance = None
    _use_onnx = False

    def __new__(cls, model_name="BAAI/bge-reranker-v2-m3", use_onnx=False):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.model_name = model_name
            cls._instance.device = "cuda" if torch.cuda.is_available() else "cpu"
            cls._instance.use_onnx = use_onnx and not torch.cuda.is_available()  # ONNX только на CPU

            logger.info(f"Loading reranker model {model_name} on {cls._instance.device} (ONNX: {cls._instance.use_onnx})")

            if cls._instance.use_onnx:
                try:
                    from optimum.onnxruntime import ORTModelForSequenceClassification
                    cls._instance.tokenizer = AutoTokenizer.from_pretrained(
                        model_name, cache_dir=MODELS_CACHE_DIR
                    )
                    cls._instance.model = ORTModelForSequenceClassification.from_pretrained(
                        model_name, cache_dir=MODELS_CACHE_DIR
                    )
                    logger.info("ONNX Runtime loaded successfully")
                except Exception as e:
                    logger.warning(f"Failed to load ONNX, falling back to PyTorch: {e}")
                    cls._instance.use_onnx = False

            if not cls._instance.use_onnx:
                cls._instance.tokenizer = AutoTokenizer.from_pretrained(
                    model_name, cache_dir=MODELS_CACHE_DIR
                )
                cls._instance.model = AutoModelForSequenceClassification.from_pretrained(
                    model_name, cache_dir=MODELS_CACHE_DIR, num_labels=1
                ).to(cls._instance.device)
                cls._instance.model.eval()

            # Предкомпилированные шаблоны для частых размеров батчей
            cls._instance._warmup()

        return cls._instance

    def _warmup(self):
        """Разогрев модели для стабильных замеров времени"""
        dummy_query = "рак легкого IIIA стадии"
        dummy_docs = ["текст рекомендации"] * 3
        try:
            self.rerank(dummy_query, dummy_docs, top_k=3)
            logger.info("Reranker warmup completed")
        except Exception as e:
            logger.warning(f"Warmup failed (non-critical): {e}")

    def rerank(self, query, documents, top_k=3, return_indices=False, batch_size=8):
        """
        Переранжирует документы с батчевой обработкой для оптимизации.

        Args:
            query: текст запроса
            documents: список документов
            top_k: сколько топовых результатов вернуть
            return_indices: вернуть индексы или тексты
            batch_size: размер батча для обработки (оптимально 8-16)
        """
        if not documents:
            return [] if return_indices else []

        # Батчевая обработка для снижения overhead
        all_scores = []
        for i in range(0, len(documents), batch_size):
            batch_docs = documents[i:i+batch_size]
            batch_pairs = [[query, doc] for doc in batch_docs]

            inputs = self.tokenizer(
                batch_pairs,
                padding=True,
                truncation=True,
                return_tensors='pt',
                max_length=512
            )

            if not self.use_onnx:
                inputs = {k: v.to(self.device) for k, v in inputs.items()}

            with torch.no_grad():
                if self.use_onnx:
                    outputs = self.model(**inputs)
                    scores = outputs.logits.squeeze(-1).tolist()
                else:
                    outputs = self.model(**inputs)
                    scores = outputs.logits.squeeze(-1).cpu().tolist()

            all_scores.extend(scores if isinstance(scores, list) else [scores])

        sorted_indices = sorted(range(len(all_scores)), key=lambda i: all_scores[i], reverse=True)
        top_indices = sorted_indices[:top_k]

        if return_indices:
            return top_indices
        else:
            return [documents[i] for i in top_indices]

    def estimate_time(self, n_docs):
        """Оценка времени работы в секундах (для принятия решения о fallback)"""
        if self.use_onnx:
            return n_docs * 0.02  # ~20ms на документ
        elif self.device == "cuda":
            return n_docs * 0.03  # ~30ms на документ
        else:
            return n_docs * 0.15  # ~150ms на документ (CPU PyTorch)

