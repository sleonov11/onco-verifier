# src/retrieval.py
import json
import hashlib
import chromadb
from chromadb.utils import embedding_functions
from rank_bm25 import BM25Okapi
from typing import List, Tuple, Optional, Dict
from .config import CHROMA_PERSIST_DIR, logger
from .models import EmbeddingModel
from .nosology_map import get_all_synonyms, get_nosology_group


class OptimizedGuidelineRetriever:
    """
    Гибридный поиск с:
    - Фильтрацией по метаданным (с поддержкой $or, $contains)
    - Умным fallback (ослабление фильтра, затем без фильтра)
    - Кэшированием запросов
    - Порогом отсечения по скору
    """

    def __init__(self, collection_name="clinical_guidelines"):
        self.embed_model = EmbeddingModel()
        self.chroma_client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
        self.collection_name = collection_name
        self._query_cache = {}
        self._cache_hits = 0

        # Подключаемся к коллекции
        try:
            self.collection = self.chroma_client.get_collection(name=collection_name)
            logger.info(f"Loaded existing collection '{collection_name}'")
        except Exception as e:
            if "does not exist" in str(e).lower():
                logger.info(f"Creating new collection '{collection_name}'")
                self.collection = self.chroma_client.create_collection(
                    name=collection_name,
                    embedding_function=embedding_functions.SentenceTransformerEmbeddingFunction(
                        model_name=self.embed_model.model_name,
                        device=self.embed_model.device
                    )
                )
            else:
                raise

        # Загружаем документы для BM25
        try:
            all_docs = self.collection.get(include=["documents", "metadatas"])
            self.doc_texts = all_docs["documents"]
            self.doc_metadatas = all_docs["metadatas"]
            self.doc_ids = all_docs["ids"]
            self.bm25_index = BM25Okapi([t.split() for t in self.doc_texts])
            logger.info(f"Loaded {len(self.doc_texts)} documents for BM25")
        except Exception as e:
            logger.warning(f"Could not load documents for BM25 (collection may be empty): {e}")
            self.doc_texts = []
            self.doc_metadatas = []
            self.doc_ids = []
            self.bm25_index = None

        self.metadata_index: Dict[str, Dict[str, set]] = {}  # для быстрой фильтрации BM25

    def index_chunks(self, chunks_file: str):
        """Индексация из JSONL-файла (используется при первоначальной загрузке)."""
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

        # Загружаем в Chroma батчами
        batch_size = 100
        for i in range(0, len(texts), batch_size):
            end = min(i + batch_size, len(texts))
            self.collection.add(
                documents=texts[i:end],
                metadatas=metadatas[i:end],
                ids=ids[i:end]
            )
            logger.info(f"Added batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1} to Chroma")

        # Обновляем локальные данные для BM25
        self.doc_texts = texts
        self.doc_ids = ids
        self.doc_metadatas = metadatas
        self.bm25_index = BM25Okapi([text.split() for text in texts])
        self._build_metadata_index()
        logger.info(f"BM25 index built with {len(texts)} documents")

    def _build_metadata_index(self):
        """Индекс для быстрой фильтрации BM25 по метаданным."""
        for idx, metadata in enumerate(self.doc_metadatas):
            for field, value in metadata.items():
                if isinstance(value, list):
                    continue
                if field not in self.metadata_index:
                    self.metadata_index[field] = {}
                if value not in self.metadata_index[field]:
                    self.metadata_index[field][value] = set()
                self.metadata_index[field][value].add(idx)

    def _bm25_search(self, query: str, top_k: int, metadata_filters: Optional[Dict] = None) -> List[Tuple[str, float, int]]:
        """BM25 поиск с фильтрацией по метаданным (только простые равенства)."""
        if not self.bm25_index:
            return []

        candidate_indices = None
        if metadata_filters:
            # Фильтрация через пересечение множеств
            for field, value in metadata_filters.items():
                if field in self.metadata_index and value in self.metadata_index[field]:
                    if candidate_indices is None:
                        candidate_indices = self.metadata_index[field][value].copy()
                    else:
                        candidate_indices &= self.metadata_index[field][value]
                else:
                    return []  # нет документов с таким значением

            if not candidate_indices:
                return []

        tokenized_query = query.split()
        all_scores = self.bm25_index.get_scores(tokenized_query)

        if candidate_indices is not None:
            scored = [(i, all_scores[i]) for i in candidate_indices if all_scores[i] > 0]
        else:
            scored = [(i, all_scores[i]) for i, score in enumerate(all_scores) if score > 0]

        scored.sort(key=lambda x: x[1], reverse=True)
        top_indices = scored[:top_k]
        return [(self.doc_texts[i], score, i) for i, score in top_indices]

    def _chroma_search(self, query: str, top_k: int, metadata_filters: Optional[Dict] = None) -> List[Tuple[str, float, dict, str]]:
        """Векторный поиск в Chroma с поддержкой сложных фильтров."""
        query_emb = self.embed_model.encode_queries([query])[0]
        where_clause = self._build_where_clause(metadata_filters)
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
        """Преобразует словарь фильтров в where-clause для Chroma."""
        if not metadata_filters:
            return None
        if "$or" in metadata_filters or "$and" in metadata_filters:
            return metadata_filters

        conditions = []
        for key, value in metadata_filters.items():
            if value is not None:
                conditions.append({key: {"$eq": value}})

        if not conditions:
            return None
        if len(conditions) == 1:
            return conditions[0]
        return {"$and": conditions}

    def search(self, query_text: str, metadata_filters: Optional[Dict] = None,
               top_k: int = 20, bm25_weight: float = 0.3,
               use_cache: bool = True, score_threshold: float = 0.6) -> List[Tuple[str, dict, float, str]]:
        """
        Основной метод гибридного поиска.

        Args:
            query_text: текст запроса
            metadata_filters: фильтры по метаданным (могут содержать "nosology")
            top_k: сколько результатов вернуть
            bm25_weight: вес BM25 (0..1)
            use_cache: использовать кэш
            score_threshold: минимальный итоговый скор (0..1)

        Returns:
            список кортежей (текст, метаданные, скор, doc_id)
        """
        cache_key = f"{query_text}|{str(metadata_filters)}|{top_k}|{bm25_weight}|{score_threshold}"
        if use_cache and cache_key in self._query_cache:
            self._cache_hits += 1
            logger.debug(f"Cache hit ({self._cache_hits} total)")
            return self._query_cache[cache_key]

        query_emb = self.embed_model.encode_queries([query_text])[0]

        # --- УНИВЕРСАЛЬНЫЙ РАСШИРЕННЫЙ ФИЛЬТР ПО НОЗОЛОГИИ ---
        original_filters = metadata_filters.copy() if metadata_filters else None
        if metadata_filters and "nosology" in metadata_filters:
            nosology = metadata_filters.pop("nosology")
            synonyms = get_all_synonyms(nosology)

            if synonyms:
                or_conditions = []
                # Точные совпадения
                for syn in synonyms:
                    or_conditions.append({"nosology": {"$eq": syn}})
                # Частичные совпадения (по последнему слову и целой фразе)
                for syn in synonyms:
                    last_word = syn.split()[-1]
                    if len(last_word) > 3:
                        or_conditions.append({"nosology": {"$contains": last_word}})
                        or_conditions.append({"source": {"$contains": last_word}})
                    if len(syn) > 4:
                        or_conditions.append({"nosology": {"$contains": syn.lower()}})
                        or_conditions.append({"source": {"$contains": syn.lower()}})
                # Группа (если есть поле nosology_group)
                group = get_nosology_group(nosology)
                if group and group != nosology:
                    or_conditions.append({"nosology_group": {"$contains": group.split()[-1]}})
                    or_conditions.append({"nosology_group": {"$eq": group}})

                metadata_filters = {"$or": or_conditions}
                logger.info(f"[RELAXED OR FILTER] Created {len(or_conditions)} conditions for nosology '{nosology}'")
            else:
                # Если синонимов нет, возвращаем как было
                metadata_filters["nosology"] = nosology

        # --- ОПРЕДЕЛЯЕМ СЛОЖНОСТЬ ФИЛЬТРА ДЛЯ BM25 ---
        has_complex_ops = metadata_filters and ("$or" in metadata_filters or "$and" in metadata_filters)

        if has_complex_ops:
            logger.debug("[SEARCH] Complex filter detected, using Chroma-only search")
            bm25_results = []
        else:
            bm25_results = self._bm25_search(query_text, top_k, metadata_filters)

        # --- CHROMA ПОИСК ---
        chroma_results = self.collection.query(
            query_embeddings=[query_emb.tolist()],
            n_results=top_k * 2,
            where=metadata_filters,
            include=['documents', 'metadatas', 'distances']
        )

        # Логируем найденные нозологии
        if chroma_results['metadatas'] and chroma_results['metadatas'][0]:
            found = [m.get('nosology', '—') for m in chroma_results['metadatas'][0]]
            logger.info(f"[FOUND NOSOLOGIES] {set(found)}")

        # --- УМНЫЙ FALLBACK (пошаговое ослабление фильтра) ---
        if len(chroma_results['documents'][0]) == 0 and original_filters and "nosology" in original_filters:
            logger.warning("[FALLBACK] No results with full OR filter, trying partial match by group")
            group = get_nosology_group(original_filters["nosology"])
            if group:
                last_word = group.split()[-1]
                fallback_filter = {"$or": [
                    {"nosology": {"$contains": last_word}},
                    {"source": {"$contains": last_word}}
                ]}
                chroma_results = self.collection.query(
                    query_embeddings=[query_emb.tolist()],
                    n_results=top_k * 2,
                    where=fallback_filter,
                    include=['documents', 'metadatas', 'distances']
                )
                logger.info(f"[FALLBACK] Partial filter applied: {fallback_filter}")

        # --- ПОЛНЫЙ FALLBACK (без фильтра) ---
        if len(chroma_results['documents'][0]) == 0:
            logger.warning("[FALLBACK] Still no results, searching without any nosology filter")
            chroma_results = self.collection.query(
                query_embeddings=[query_emb.tolist()],
                n_results=top_k * 2,
                where=None,
                include=['documents', 'metadatas', 'distances']
            )

        # --- ОБЪЕДИНЕНИЕ РЕЗУЛЬТАТОВ (Chroma + BM25) ---
        combined_scores = {}
        doc_info = {}

        # Chroma
        chroma_chunks = list(zip(
            chroma_results['documents'][0] if chroma_results['documents'] else [],
            chroma_results['distances'][0] if chroma_results['distances'] else [],
            chroma_results['metadatas'][0] if chroma_results['metadatas'] else [],
            chroma_results['ids'][0] if chroma_results['ids'] else []
        ))

        for text, dist, meta, doc_id in chroma_chunks:
            score = 1 / (1 + dist)  # преобразуем расстояние в скор
            combined_scores[doc_id] = combined_scores.get(doc_id, 0) + (1 - bm25_weight) * score
            doc_info[doc_id] = (text, meta)

        # BM25
        if bm25_results:
            max_bm25 = max(s for _, s, _ in bm25_results) if bm25_results else 1.0
            for text, score, idx in bm25_results:
                doc_id = self.doc_ids[idx]
                norm_score = score / max_bm25
                combined_scores[doc_id] = combined_scores.get(doc_id, 0) + bm25_weight * norm_score
                doc_info[doc_id] = (text, self.doc_metadatas[idx])

        # Сортируем и отсекаем по порогу
        sorted_docs = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)
        result = []
        for doc_id, score in sorted_docs:
            if score < score_threshold:
                continue
            text, meta = doc_info[doc_id]
            result.append((text, meta, score, doc_id))
            if len(result) >= top_k:
                break

        # Если после всех попыток результат пуст – повторяем без фильтра (на всякий случай)
        if not result and original_filters:
            logger.warning("[FINAL FALLBACK] No results after all attempts, retrying without any filters")
            return self.search(query_text, None, top_k, bm25_weight, use_cache, score_threshold)

        # Кэшируем
        if use_cache:
            if len(self._query_cache) > 1000:
                self._query_cache.clear()
            self._query_cache[cache_key] = result

        return result