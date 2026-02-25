# src/normalizer.py
import hashlib
import json
from typing import Dict, Optional, Any


def _sanitize_metadata_value(value: Any) -> Any:
    """Преобразует значение метаданных в допустимый для Chroma тип"""
    if value is None:
        return ""  # пустая строка вместо None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        # Словарь преобразуем в JSON-строку
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (list, tuple)):
        # Список преобразуем в строку через запятую
        return ", ".join(str(v) for v in value)
    # Всё остальное приводим к строке
    return str(value)


def normalize_chunk(chunk: Dict) -> Optional[Dict]:
    """
    Приводит чанк из любого формата к единому:
    - id (если нет, генерируется)
    - text (строка)
    - metadata (словарь с плоскими полями допустимых типов)
    """
    # 1. Извлекаем текст
    text = None
    raw_text = chunk.get("text")
    if isinstance(raw_text, str):
        text = raw_text
    elif isinstance(raw_text, dict):
        # RUSSCO формат: ищем original_guideline_text или doctor_version
        text = raw_text.get("original_guideline_text") or raw_text.get("doctor_version")
    if not text:
        return None

    # 2. Метаданные
    metadata = chunk.get("metadata", {}).copy()

    # Разворачиваем molecular_markers, если есть
    if "molecular_markers" in metadata and isinstance(metadata["molecular_markers"], dict):
        markers = metadata.pop("molecular_markers")
        for k, v in markers.items():
            clean_key = k.replace("-", "_").replace(" ", "_")
            metadata[f"marker_{clean_key}"] = v

    # 3. Добавляем источник, если есть в корне (RUSSCO формат)
    if "source" in chunk and isinstance(chunk["source"], dict):
        src = chunk["source"]
        metadata["source"] = src.get("guideline", "Unknown")
        metadata["section"] = src.get("section", "")
        metadata["page"] = src.get("page")
        metadata["country"] = src.get("country", "ru")

    # 4. Очищаем метаданные
    cleaned_metadata = {}
    for key, value in metadata.items():
        if value is not None:
            cleaned_metadata[key] = _sanitize_metadata_value(value)

    # Приводим строковые метаданные к нижнему регистру для регистронезависимого поиска
    for key, value in cleaned_metadata.items():
        if isinstance(value, str):
            cleaned_metadata[key] = value.lower()
    # 5. ID
    chunk_id = chunk.get("id")
    if not chunk_id:
        chunk_id = hashlib.sha256(text.encode()).hexdigest()[:16]

    return {
        "id": chunk_id,
        "text": text,
        "metadata": cleaned_metadata
    }