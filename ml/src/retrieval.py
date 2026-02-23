import json
import hashlib
import chromadb
from chromadb.utils import embedding_functions
from rank_bm25 import BM25Okapi
import numpy as np
from typing import List, Tuple, Optional, Dict
from .config import CHROMA_PERSIST_DIR, logger
from .models import EmbeddingModel


class OptimizedGuidelineRetriever:
    """
    Улучшенный retriever с:
    - Фильтрацией по метаданным в Chroma
    - Умным fallback на полный поиск при пустых результатах
    - Кэшированием частых запросов
    """

    def __init__(self, collection_name="clinical_guidelines"):
        self.embed_model = EmbeddingModel()
        self.chroma_client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
        self.collection_name = collection_name
        self._query_cache = {}  # Простой LRU-кэш для повторных запросов
        self._cache_hits = 0

        # Подключение к коллекции
        try:
            self.collection = self.chroma_client.get_collection(name=collection_name)
            logger.info(f"Loaded existing collection '{collection_name}'")
        except Exception as e:
            if "does not exist" in str(e).lower():
                logger.info(f"Creating new collection '{collection_name}'")
                self.collection = self.chroma_client.create_collection(
                    name=collection_name,
                    embedding_function=embedding_functions.SentenceTransformerEmbeddingFunction(
                        model_name="intfloat/multilingual-e5-large-instruct"
                    )
                )
            else:
                raise

        self.bm25_index: Optional[BM25Okapi] = None
        self.doc_texts: List[str] = []
        self.doc_ids: List[str] = []
        self.doc_metadatas: List[dict] = []

        # Индексы для быстрой фильтрации BM25
        self.metadata_index: Dict[str, Dict[str, set]] = {}  # {field: {value: set(indices)}}

    def index_chunks(self, chunks_file: str):
        """Индексация с построением вспомогательных индексов"""
        texts = []
        ids = []
        metadatas = []

        with open(chunks_file, 'r', encoding='utf-8') as f:
            for line in f:
                chunk = json.loads(line)
                text = chunk['text']
                doc_id = chunk.get('id', hashlib.sha256(text.encode()).hexdigest())
                metadata = chunk.get('metadata', {})
                texts.append(text)
                ids.append(doc_id)
                metadatas.append(metadata)

        # Добавляем в Chroma батчами
        batch_size = 100
        for i in range(0, len(texts), batch_size):
            end = min(i + batch_size, len(texts))
            self.collection.add(
                documents=texts[i:end],
                metadatas=metadatas[i:end],
                ids=ids[i:end]
            )
            logger.info(f"Added batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1} to Chroma")

        # Сохраняем для BM25 и строим индексы
        self.doc_texts = texts
        self.doc_ids = ids
        self.doc_metadatas = metadatas
        self.bm25_index = BM25Okapi([text.split() for text in texts])

        # Строим индексы по метаданным для быстрой фильтрации
        self._build_metadata_index()

        logger.info(f"BM25 index built with {len(texts)} documents")
        logger.info(f"Metadata fields indexed: {list(self.metadata_index.keys())}")

    def _build_metadata_index(self):
        """Строит инвертированные индексы по метаданным для BM25"""
        for idx, metadata in enumerate(self.doc_metadatas):
            for field, value in metadata.items():
                if field not in self.metadata_index:
                    self.metadata_index[field] = {}
                if value not in self.metadata_index[field]:
                    self.metadata_index[field][value] = set()
                self.metadata_index[field][value].add(idx)

    def _bm25_search(self, query: str, top_k: int, metadata_filters: Optional[Dict] = None) -> List[Tuple[str, float, int]]:
        """
        BM25 с возможностью фильтрации по метаданным.
        При фильтрации ищем только в подходящих документах.
        """
        if not self.bm25_index:
            return []

        # Определяем кандидатов для поиска
        candidate_indices = None
        if metadata_filters:
            # Пересечение множеств по всем фильтрам
            for field, value in metadata_filters.items():
                if field in self.metadata_index and value in self.metadata_index[field]:
                    if candidate_indices is None:
                        candidate_indices = self.metadata_index[field][value].copy()
                    else:
                        candidate_indices &= self.metadata_index[field][value]
                else:
                    # Нет документов с таким фильтром
                    return []

            if not candidate_indices:
                return []

        # Получаем скоры для всех документов
        tokenized_query = query.split()
        all_scores = self.bm25_index.get_scores(tokenized_query)

        # Фильтруем если нужно
        if candidate_indices is not None:
            scored_candidates = [(i, all_scores[i]) for i in candidate_indices if all_scores[i] > 0]
        else:
            scored_candidates = [(i, score) for i, score in enumerate(all_scores) if score > 0]

        # Сортируем и берем топ-k
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        top_indices = scored_candidates[:top_k]

        return [(self.doc_texts[i], score, i) for i, score in top_indices]

    def _chroma_search(self, query: str, top_k: int, metadata_filters: Optional[Dict] = None) -> List[Tuple[str, float, dict, str]]:
        """Chroma с фильтрацией по метаданным через where clause"""
        query_emb = self.embed_model.encode_queries([query])[0]

        # Формируем where clause для Chroma
        where_clause = self._build_where_clause(metadata_filters)

        # Запрашиваем с запасом, т.к. фильтр может сильно сократить выборку
        n_results = top_k * 3 if metadata_filters else top_k

        results = self.collection.query(
            query_embeddings=[query_emb.tolist()],
            n_results=n_results,
            where=where_clause,
            include=['documents', 'metadatas', 'distances']
        )

        documents = results['documents'][0] if results['documents'] else []
        metadatas = results['metadatas'][0] if results['metadatas'] else []
        distances = results['distances'][0] if results['distances'] else []
        ids = results['ids'][0] if results['ids'] else []

        return list(zip(documents, distances, metadatas, ids))[:top_k]

    def _build_where_clause(self, metadata_filters: Optional[Dict]) -> Optional[Dict]:
        """Строит where clause для Chroma из фильтров"""
        if not metadata_filters:
            return None

        conditions = []
        for key, value in metadata_filters.items():
            if value is not None:
                conditions.append({key: {"$eq": value}})

        if not conditions:
            return None
        elif len(conditions) == 1:
            return conditions[0]
        else:
            return {"$and": conditions}

    def search(self, query_text: str, metadata_filters: Optional[Dict] = None,
               top_k: int = 20, bm25_weight: float = 0.3,
               use_cache: bool = True) -> List[Tuple[str, dict, float, str]]:
        """
        Гибридный поиск с фильтрацией и кэшированием.

        Стратегия:
        1. Если есть фильтры — применяем к обеим системам
        2. Если после фильтрации мало результатов — пробуем поиск без фильтров
        3. Объединяем через взвешенную сумму
        """
        # Проверка кэша
        cache_key = f"{query_text}|{str(metadata_filters)}|{top_k}"
        if use_cache and cache_key in self._query_cache:
            self._cache_hits += 1
            logger.debug(f"Cache hit ({self._cache_hits} total)")
            return self._query_cache[cache_key]

        # Поиск с фильтрами
        bm25_results = self._bm25_search(query_text, top_k, metadata_filters)
        chroma_results = self._chroma_search(query_text, top_k, metadata_filters)

        # Fallback: если фильтры слишком агрессивны и нет результатов
        if metadata_filters and (not bm25_results or not chroma_results):
            logger.warning(f"Filtered search returned few results, trying without filters")
            bm25_results = self._bm25_search(query_text, top_k, None)
            chroma_results = self._chroma_search(query_text, top_k, None)

        # Объединение результатов
        combined_scores = {}
        doc_info = {}

        # BM25: нормализуем скоры (максимум ~10-15 для длинных документов)
        if bm25_results:
            max_bm25 = max(score for _, score, _ in bm25_results) if bm25_results else 1.0
            for text, score, idx in bm25_results:
                doc_id = self.doc_ids[idx]
                normalized_score = score / max_bm25 if max_bm25 > 0 else 0
                combined_scores[doc_id] = combined_scores.get(doc_id, 0) + bm25_weight * normalized_score
                doc_info[doc_id] = (text, self.doc_metadatas[idx])

        # Chroma: расстояние косинусное → скор (0..1)
        for text, dist, meta, doc_id in chroma_results:
            score = 1 / (1 + dist)  # чем меньше расстояние, тем выше скор
            combined_scores[doc_id] = combined_scores.get(doc_id, 0) + (1 - bm25_weight) * score
            if doc_id not in doc_info:  # BM25 мог уже добавить
                doc_info[doc_id] = (text, meta)

        # Сортируем по убыванию комбинированного скора
        sorted_docs = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        result = []
        for doc_id, score in sorted_docs:
            text, meta = doc_info[doc_id]
            result.append((text, meta, score, doc_id))

        # Сохраняем в кэш (LRU с ограничением размера)
        if use_cache:
            if len(self._query_cache) > 1000:
                self._query_cache.clear()
            self._query_cache[cache_key] = result

        return result

