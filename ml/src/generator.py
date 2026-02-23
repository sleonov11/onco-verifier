# src/generator.py — обновить импорты и логику
import json
import re
import hashlib
from typing import List, Optional, Dict
from .config import LLM_PROVIDER, logger
from .schemas import Issue

_cache = {}


def generate_response(query_text: str, retrieved_chunks: list, role: str,
                      locale: str, request_id: str = None,
                      deterministic_issues: Optional[List[Issue]] = None) -> Dict:
    # Формируем контекст
    context_parts = []
    sources = []
    for i, (text, metadata, score, doc_id) in enumerate(retrieved_chunks[:5]):
        source = metadata.get('source', 'Unknown')
        section = metadata.get('section', '')
        context_parts.append(f"[{i + 1}] {source} ({section}):\n{text[:500]}")
        sources.append({
            "source": source,
            "section": section,
            "text": text[:200] + "...",
            "doc_id": doc_id
        })

    context = "\n\n".join(context_parts)

    cache_key = hashlib.sha256(f"{query_text}|{context}|{role}|{LLM_PROVIDER}".encode()).hexdigest()
    if cache_key in _cache:
        result = _cache[cache_key].copy()
        result["_sources"] = sources
        return result

    # генерация
    try:
        if LLM_PROVIDER == "gigachat":
            result = _generate_gigachat(query_text, context, role, locale, deterministic_issues)
        elif LLM_PROVIDER == "openai":
            result = _generate_openai(query_text, context, role, locale, deterministic_issues)
        else:
            result = _generate_rule_based(query_text, context, role, locale, deterministic_issues)
    except Exception as e:
        logger.error(f"LLM error: {e}")
        result = _generate_rule_based(query_text, context, role, locale, deterministic_issues)

    result["_sources"] = sources
    _cache[cache_key] = result.copy()
    return result


def _generate_gigachat(query_text, context, role, locale, deterministic_issues):
    from .gigachat_client import get_gigachat

    system = f"Ты эксперт-онколог. Отвечай JSON. Язык: {locale}."

    issues_text = ""
    if deterministic_issues:
        issues_text = "\nВЫЯВЛЕННЫЕ ПРОБЛЕМЫ:\n" + "\n".join([
            f"- [{i.severity}] {i.code}: {i.title}" for i in deterministic_issues
        ])

    user = f"""Данные: {query_text}

Рекомендации: {context}{issues_text}

Ответь JSON:
{{
  "is_compliant": true/false,
  "summary": "кратко",
  "issues": [{{"code": "...", "severity": "...", "title": "...", "details": "...", "suggested_action": "...", "confidence": 0.9}}],
  "doctor_explanation": "...",
  "patient_explanation": "...",
  "recommended_next_steps": ["..."]
}}"""

    client = get_gigachat()
    content = client.generate(system, user, 0.0)

    # Парсинг джейсончика
    for pattern in [r"```json\s*(.*?)\s*```", r"```\s*(.*?)\s*```", r"(\{[\s\S]*\})"]:
        match = re.search(pattern, content, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1).strip())
            except:
                pass

    return json.loads(content)


def _generate_rule_based(query_text, context, role, locale, deterministic_issues):
    """Fallback без LLM"""
    issues = [i.dict() if hasattr(i, 'dict') else i for i in (deterministic_issues or [])]

    is_compliant = not any(i.get('severity') in ['critical', 'high'] for i in issues)

    if is_compliant:
        return {
            "is_compliant": True,
            "summary": "Лечение соответствует рекомендациям",
            "issues": [],
            "doctor_explanation": "Детерминированные проверки пройдены.",
            "patient_explanation": "Ваше лечение соответствует стандартам.",
            "recommended_next_steps": ["Продолжить терапию"]
        }

    return {
        "is_compliant": False,
        "summary": f"Обнаружено {len(issues)} проблем",
        "issues": issues,
        "doctor_explanation": "\n".join([f"[{i['severity']}] {i['code']}" for i in issues[:3]]),
        "patient_explanation": "Выявлены важные моменты, требующие внимания врача.",
        "recommended_next_steps": list(
            set(i.get('suggested_action', '') for i in issues if i.get('suggested_action'))) or ["Обсудить с врачом"]
    }


