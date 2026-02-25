# src/generator.py
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
    """
    Генерирует ответ через GigaChat с учётом роли (врач/пациент).
    Для врача: профессионально, но мягко, с уважением к коллеге.
    Для пациента: тепло, поддерживающе, с акцентом на доверие к врачу.
    """
    # Формируем контекст из топ-5 чанков
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

    # Кэш
    cache_key = hashlib.sha256(f"{query_text}|{context}|{role}|{LLM_PROVIDER}".encode()).hexdigest()
    if cache_key in _cache:
        result = _cache[cache_key].copy()
        result["_sources"] = sources
        return result

    # Генерация
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


def _build_system_prompt(role: str, locale: str) -> str:
    """Ролевой системный промпт"""
    base = f"Язык ответа: {locale}.\n\n"

    if role == "doctor":
        return base + """Ты — эксперт-онколог, помогаешь коллеге интерпретировать результаты проверки лечения.

В начале всегда подтверждай, что коллега действует профессионально:
"Коллега, вы основываете лечение на самых передовых международных и российских клинических рекомендациях. Пациенту назначено лучшее лечение, которое сегодня предлагает современная медицина."

Затем переходи к анализу:
- Если всё хорошо: подтверди правильность выбора, отметь сильные стороны назначения.
- Если есть недочёты: указывай на них конструктивно, без обвинений. Используй формулировки: "Обратите внимание на...", "Возможно, стоит рассмотреть...", "Рекомендуется уточнить...".
- Никаких резких формулировок, слов "ошибка", "неправильно", "нарушение". Только профессиональные рекомендации.

Тон: уважительный, поддерживающий, коллегиальный."""

    else:  # patient
        return base + """Ты — доброжелательный помощник пациента. Отвечаешь на вопросы о лечении.

В начале всегда подчёркивай, что врач действует в интересах пациента и назначил лучшее лечение:
"Ваш врач основывает лечение на самых передовых международных и российских клинических рекомендациях. Вам назначено лучшее лечение, которое сегодня предлагает современная медицина."

Правила общения:
- Объясняй простым языком, без сложных медицинских терминов.
- Если пациент тревожится — успокаивай, снижай напряжение.
- Если есть вопросы по лечению — объясняй мягко, но всегда направляй к врачу за деталями.
- Никогда не говори об ошибках врача. Только: "Ваш врач может уточнить...", "Для ещё большей эффективности...".
- Вселяй уверенность: "Вы в надёжных руках", "Команда врачей рядом с вами".

Тон: добрый, заботливый, успокаивающий."""


def _build_user_prompt(query_text: str, context: str, deterministic_issues: Optional[List[Issue]]) -> str:
    """Строит пользовательский промпт с few-shot примером и жёсткими требованиями к JSON"""
    issues_text = ""
    if deterministic_issues:
        issues_text = "\n\nВЫЯВЛЕННЫЕ ПРОБЛЕМЫ (обязательно включи их в ответ, но формулируй соответственно роли):\n"
        for i in deterministic_issues:
            issues_text += f"- [{i.severity}] {i.code}: {i.title}\n"
        issues_text += "\nВажно: если severity critical или high, is_compliant должен быть false."

    # Few-shot пример корректного JSON (для стабильности)
    example_json = '''{
  "is_compliant": false,
  "summary": "Обнаружено несоответствие по мутационному статусу",
  "issues": [
    {
      "code": "MISSING_EGFR_FOR_TKI",
      "severity": "critical",
      "title": "EGFR-мутация не подтверждена для ТКИ",
      "details": "Назначена таргетная терапия (осимертиниб) без подтверждённой EGFR-сенситизирующей мутации.",
      "suggested_action": "Провести тестирование EGFR (экзоны 18-21) или сменить схему.",
      "confidence": 0.98
    }
  ],
  "doctor_explanation": "Коллега, обратите внимание, что согласно NCCN/RUSSCO 2025–2026 осимертиниб в 1-й линии показан только при подтверждённой мутации EGFR. Рекомендуется провести молекулярное тестирование.",
  "patient_explanation": "Ваш врач назначил очень современное лечение, но для его максимальной эффективности нужно уточнить один анализ. Это обычная практика – ничего страшного. Просто обсудите это с доктором.",
  "recommended_next_steps": ["Сдать тест на мутации EGFR", "Обсудить результаты с лечащим врачом"]
}'''

    return f"""Данные пациента:
{query_text}

Клинические рекомендации (используй ТОЛЬКО их):
{context}{issues_text}

Ты должен ответить **ТОЛЬКО** валидным JSON-объектом. 
Без каких-либо пояснений, без слов "Вот JSON", без markdown-блоков (```json). 
Начинай сразу с {{ и заканчивай }}. Ничего до и после.

Пример правильного ответа:
{example_json}

Теперь ответь в точно таком же формате для текущего случая:"""


def _generate_gigachat(query_text: str, context: str, role: str, locale: str,
                       deterministic_issues: Optional[List[Issue]] = None) -> Dict:
    """Генерация через GigaChat с retry-механизмом"""
    from .gigachat_client import get_gigachat

    system_prompt = _build_system_prompt(role, locale)
    user_prompt = _build_user_prompt(query_text, context, deterministic_issues)

    client = get_gigachat()

    # Первая попытка (температура 0.0)
    for attempt in range(2):
        try:
            content = client.generate(system_prompt, user_prompt, temperature=0.0 if attempt == 0 else 0.1)
            result = _extract_json(content)
            if result is not None:
                return _normalize_result(result, deterministic_issues, role)

            # Если не удалось распарсить, на второй попытке добавим жёсткое напоминание
            if attempt == 0:
                user_prompt += "\n\nПРЕДУПРЕЖДЕНИЕ: Твой предыдущий ответ не был чистым JSON. Ответь ТОЛЬКО JSON-объектом, без лишнего текста."
                logger.warning("GigaChat returned non-JSON, retrying with stricter prompt")
        except Exception as e:
            logger.warning(f"GigaChat attempt {attempt + 1} failed: {e}")

    # Если обе попытки провалились — падаем в rule-based fallback
    logger.error("GigaChat failed to produce valid JSON after 2 attempts")
    raise ValueError("Invalid JSON from GigaChat")


def _generate_rule_based(query_text: str, context: str, role: str, locale: str,
                         deterministic_issues: Optional[List[Issue]] = None) -> Dict:
    """Fallback без LLM — используем детерминированные проблемы и шаблоны"""
    issues = []
    if deterministic_issues:
        issues = [i.dict() if hasattr(i, 'dict') else i for i in deterministic_issues]

    is_compliant = not any(i.get('severity') in ['critical', 'high'] for i in issues)

    if is_compliant:
        return {
            "is_compliant": True,
            "summary": "Лечение соответствует клиническим рекомендациям",
            "issues": [],
            "doctor_explanation": "Детерминированные проверки пройдены. Назначение соответствует стандартам.",
            "patient_explanation": "Всё хорошо! Ваш врач подобрал правильное лечение в соответствии с современными стандартами. Продолжайте следовать рекомендациям.",
            "recommended_next_steps": ["Продолжить назначенное лечение"]
        }

    # Есть проблемы — формируем объяснения в зависимости от роли
    if role == "doctor":
        doctor_exp = "Коллега, обратите внимание на следующие моменты:\n"
        doctor_exp += "\n".join(
            [f"- [{i['severity']}] {i['code']}: {i.get('details', i['title'])}" for i in issues[:3]])
        if issues:
            doctor_exp += f"\n\nРекомендации:\n" + "\n".join(
                [f"- {i.get('suggested_action', '')}" for i in issues if i.get('suggested_action')])
        patient_exp = "В лечении есть небольшие уточнения, которые ваш врач обсудит с вами. Не волнуйтесь, это нормальная практика."
    else:
        doctor_exp = "\n".join([f"[{i['severity']}] {i['code']}" for i in issues[:3]])
        patient_exp = "Ваш врач подобрал лечение, но для ещё большей эффективности может уточнить некоторые детали. Это обычная практика – ничего страшного. Просто обсудите это с ним на приёме."

    steps = []
    for i in issues:
        if i.get('suggested_action'):
            steps.append(i['suggested_action'])

    return {
        "is_compliant": False,
        "summary": f"Обнаружено {len(issues)} несоответствий",
        "issues": issues,
        "doctor_explanation": doctor_exp,
        "patient_explanation": patient_exp,
        "recommended_next_steps": list(dict.fromkeys(steps)) if steps else ["Обсудить с врачом"]
    }


def _generate_openai(query_text: str, context: str, role: str, locale: str,
                     deterministic_issues: Optional[List[Issue]] = None) -> Dict:
    """Заглушка для OpenAI"""
    raise NotImplementedError("OpenAI not configured")


def _extract_json(content: str) -> Optional[Dict]:
    """
    Улучшенный парсер JSON: удаляет markdown-обёртки, ищет самый большой JSON-объект,
    пытается исправить распространённые ошибки (лишние запятые).
    """
    content = content.strip()

    # Убираем markdown-обёртки ```json ... ``` или ``` ... ```
    if content.startswith(("```json", "```")):
        # Разделяем по маркеру, берём внутренность
        parts = re.split(r"```(?:json)?", content, maxsplit=2)
        if len(parts) >= 2:
            content = parts[1].strip()
        # Убираем закрывающие ```
        if content.endswith("```"):
            content = content[:-3].strip()

    # Ищем первый '{' и последний '}'
    start = content.find("{")
    end = content.rfind("}") + 1
    if start != -1 and end > start:
        candidate = content[start:end]
        # Пробуем распарсить как есть
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            # Если не вышло, пробуем почистить: удаляем trailing commas перед } или ]
            fixed = re.sub(r",\s*([}\]])", r"\1", candidate)
            try:
                return json.loads(fixed)
            except:
                pass

    # В крайнем случае пробуем распарсить весь исходный текст
    try:
        return json.loads(content)
    except:
        return None


def _normalize_result(result: Dict, deterministic_issues: Optional[List[Issue]], role: str) -> Dict:
    """Нормализует результат, добавляет недостающие поля"""
    required = ["is_compliant", "summary", "issues", "doctor_explanation", "patient_explanation"]
    for field in required:
        if field not in result:
            if field == "is_compliant":
                result[field] = True
            elif field == "issues":
                result[field] = []
            else:
                result[field] = ""

    if "recommended_next_steps" not in result:
        result["recommended_next_steps"] = []

    # Если есть детерминированные проблемы с critical/high, принудительно ставим compliant=false
    if deterministic_issues:
        if any(i.severity in ['critical', 'high'] for i in deterministic_issues):
            result["is_compliant"] = False

    return result