from .schemas import InputData
from typing import List, Dict, Set


class QueryExpansion:
    """
    Расширение запросов медицинскими синонимами и связанными терминами.
    Повышает recall при ограниченной базе чанков.
    """

    # Расширения для препаратов (торговые названия, механизмы, классы)
    DRUG_EXPANSIONS = {
        # EGFR ТКИ
        "осимертиниб": ["тагриссо", "tagrisso", "третье поколение ТКИ", "EGFR T790M"],
        "gefitinib": ["ирресса", "iressa", "первое поколение ТКИ"],
        "erlotinib": ["тарцева", "tarceva"],

        # ALK ингибиторы
        "алектиниб": ["алесенса", "alecensa", "ALK"],
        "crizotinib": ["ксалкори", "xalkori"],

        # Иммунотерапия
        "пембролизумаб": ["кейтруда", "keytruda", "PD-1", "PD-1 ингибитор", "иммунотерапия"],
        "ниволумаб": ["опдиво", "opdivo", "PD-1"],
        "атезолизумаб": ["тесентрик", "tecentriq", "PD-L1"],
        "durvalumab": ["имфинзи", "imfinzi", "консолидация", "PACIFIC"],

        # Химиотерапия
        "цисплатин": ["cisplatin", "платина", "двойная платина"],
        "пеметрексед": ["pemetrexed", "алимта", "alimta"],
        "паклитаксел": ["paclitaxel", "таксол"],
        "доцетаксел": ["docetaxel", "таксотер"],
    }

    # Расширения для стадий
    STAGE_EXPANSIONS = {
        "IA": ["ранний", "начальная стадия", "T1N0"],
        "IB": ["ранний", "T2aN0"],
        "IIA": ["локально-распространенный", "T2bN0", "T1N1"],
        "IIB": ["локально-распространенный", "T2N1", "T3N0"],
        "IIIA": ["локально-распространенный", "N2", "T3N1", "T4N0", "T4N1", "ресебтабельный"],
        "IIIB": ["локально-распространенный", "N3", "T3N2", "нересеабельный"],
        "IIIC": ["локально-распространенный", "T4N2", "T3N3", "T4N3", "нересеабельный"],
        "IV": ["метастатический", "m1", "отдаленные метастазы", "продвинутый"],
        "IVA": ["метастатический", "M1a", "плевральный выпот", "перикардиальный выпот"],
        "IVB": ["метастатический", "M1b", "одиночный метастаз", "олигометастатический"],
        "IVC": ["метастатический", "M1c", "множественные метастазы"],
    }

    # Расширения для нозологий
    NOSOLOGY_EXPANSIONS = {
        "рак легкого": ["немелкоклеточный рак легкого", "НМРЛ", "NSCLC", "C34", "легкое"],
        "рак молочной железы": ["РМЖ", "HER2", "HR+", "трипл-негативный", "C50"],
    }

    # Маркеры и их синонимы
    MARKER_EXPANSIONS = {
        "EGFR": ["EGFR-мутация", "экзон 19 делеция", "L858R", "T790M", "сенситизирующая мутация"],
        "ALK": ["ALK-реаранжировка", "EML4-ALK", "ALK-позитивный"],
        "PD-L1": ["PD-L1", "TPS", "выражение PD-L1", "иммунный статус"],
        "HER2": ["HER2-положительный", "IHC 3+", "ISH+", "ERBB2"],
    }

    @classmethod
    def expand_drug(cls, drug_name: str) -> List[str]:
        """Расширяет название препарата синонимами"""
        drug_lower = drug_name.lower()
        expansions = [drug_name]  # оригинальное название

        for key, values in cls.DRUG_EXPANSIONS.items():
            if key in drug_lower or drug_lower in key:
                expansions.extend(values)

        return list(set(expansions))  # уникальные значения

    @classmethod
    def expand_stage(cls, stage: str) -> List[str]:
        """Расширяет стадию синонимами"""
        if not stage:
            return []

        stage_upper = stage.upper().replace("СТАДИЯ", "").replace("STAGE", "").strip()
        expansions = [stage]

        for key, values in cls.STAGE_EXPANSIONS.items():
            if key in stage_upper or stage_upper in key:
                expansions.extend(values)

        return list(set(expansions))

    @classmethod
    def expand_nosology(cls, nosology: str) -> List[str]:
        """Расширяет нозологию синонимами"""
        if not nosology:
            return []

        nos_lower = nosology.lower()
        expansions = [nosology]

        for key, values in cls.NOSOLOGY_EXPANSIONS.items():
            if key in nos_lower:
                expansions.extend(values)

        return list(set(expansions))


def build_query_from_input(input_data: InputData, use_expansion: bool = True) -> str:
    """
    Преобразует структурированные данные в текстовый запрос для поиска.
    С опциональным расширением синонимами для повышения recall.
    """
    parts = []
    expansion = QueryExpansion() if use_expansion else None

    # Диагноз с расширением
    diag = input_data.diagnosis
    diag_terms = [diag.name]
    if use_expansion and diag.name:
        diag_terms.extend(expansion.expand_nosology(diag.name))

    diag_str = f"Диагноз: {', '.join(set(diag_terms))}"
    if diag.icd10:
        diag_str += f" (МКБ-10: {diag.icd10})"
    parts.append(diag_str)

    # Стадия с расширением
    if diag.stage:
        stage_terms = [diag.stage]
        if use_expansion:
            stage_terms.extend(expansion.expand_stage(diag.stage))
        parts.append(f"Стадия: {', '.join(set(stage_terms))}")

    if diag.tnm:
        parts.append(f"TNM: {diag.tnm}")
    if diag.histology:
        parts.append(f"Гистология: {diag.histology}")

    # Молекулярные маркеры с расширением
    if input_data.molecular_markers:
        markers_parts = []
        markers = input_data.molecular_markers

        if markers.EGFR:
            egfr_terms = [f"EGFR: {markers.EGFR}"]
            if use_expansion and markers.EGFR.lower() in ["positive", "mutated", "mutant", "да"]:
                egfr_terms.extend(["EGFR-мутация", "сенситизирующая мутация", "экзон 19", "L858R"])
            markers_parts.append(", ".join(set(egfr_terms)))

        if markers.ALK:
            alk_terms = [f"ALK: {markers.ALK}"]
            if use_expansion and markers.ALK.lower() in ["positive", "rearranged", "fusion", "да"]:
                alk_terms.extend(["ALK-реаранжировка", "EML4-ALK"])
            markers_parts.append(", ".join(set(alk_terms)))

        if markers.PD_L1:
            markers_parts.append(f"PD-L1: {markers.PD_L1}")

        if markers_parts:
            parts.append("Маркеры: " + "; ".join(markers_parts))

    # Контекст пациента
    ctx = input_data.patient_context
    context_parts = []
    if ctx.age:
        context_parts.append(f"возраст {ctx.age}")
    if ctx.sex:
        context_parts.append(f"пол {ctx.sex}")
    if ctx.comorbidities:
        context_parts.append(f"сопутствующие: {', '.join(ctx.comorbidities)}")
    if ctx.symptoms:
        context_parts.append(f"симптомы: {', '.join(ctx.symptoms)}")

    if context_parts:
        parts.append("Пациент: " + "; ".join(context_parts))

    # Лечение с расширением препаратов
    tr = input_data.treatment
    therapy_parts = [f"линия терапии: {tr.therapy_line}"]

    if tr.proposed_regimen:
        regimen_terms = []
        for drug in tr.proposed_regimen:
            drug_terms = [drug]
            if use_expansion:
                drug_terms.extend(expansion.expand_drug(drug))
            regimen_terms.append("/".join(set(drug_terms[:3])))  # ограничиваем для краткости

        therapy_parts.append(f"схема: {', '.join(regimen_terms)}")

    if tr.cycle:
        therapy_parts.append(f"цикл: {tr.cycle}")
    if tr.notes:
        therapy_parts.append(f"примечания: {tr.notes}")

    parts.append("Лечение: " + "; ".join(therapy_parts))

    # Свободный текст (если есть)
    if input_data.free_text:
        if input_data.free_text.doctor_notes:
            parts.append(f"Примечания врача: {input_data.free_text.doctor_notes}")
        if input_data.free_text.patient_message:
            parts.append(f"Сообщение пациента: {input_data.free_text.patient_message}")

    query = ". ".join(parts)

    # Дополнительное расширение для ключевых комбинаций
    if use_expansion:
        query = _add_contextual_expansions(query, input_data)

    return query


def _add_contextual_expansions(query: str, input_data: InputData) -> str:
    """
    Добавляет контекстуальные расширения на основе комбинаций параметров.
    Например, для IIIA + Пембролизумаб добавляет 'консолидация' или 'неоадъювант'.
    """
    expansions = []

    # Контекст для стадии III
    if input_data.diagnosis.stage and "III" in input_data.diagnosis.stage.upper():
        if any("пембролизумаб" in r.lower() for r in input_data.treatment.proposed_regimen):
            if input_data.treatment.therapy_line == 1:
                expansions.extend(["неоадъювант", "переоперационная"])
            else:
                expansions.extend(["консолидация", "PACIFIC"])

        if any("durvalumab" in r.lower() or "дурвалумаб" in r.lower() for r in input_data.treatment.proposed_regimen):
            expansions.extend(["консолидация после химиолучения", "PACIFIC"])

    # Контекст для метастатического (IV стадия)
    if input_data.diagnosis.stage and ("IV" in input_data.diagnosis.stage.upper() or "4" in input_data.diagnosis.stage):
        expansions.extend(["первая линия", "паллиативная", "системная терапия"])

    # Контекст для EGFR+
    if input_data.molecular_markers and input_data.molecular_markers.EGFR:
        if input_data.molecular_markers.EGFR.lower() in ["positive", "mutated", "да"]:
            expansions.extend(["таргетная терапия", "ТКИ первой линии", "осимертиниб"])

    if expansions:
        query += " [" + ", ".join(set(expansions)) + "]"

    return query


# Обратная совместимость
build_query = build_query_from_input


