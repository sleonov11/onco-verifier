from .schemas import InputData, Issue, Treatment
from typing import List, Optional, Dict, Set
import re


class TherapyValidator:
    """
    Детерминированный валидатор для проверки базовых клинических правил.
    Не использует LLM — работает только на основе структурированных данных.
    """

    # Таргетная терапия ТКИ требует EGFR-мутации
    EGFR_TKIS = {
        "осимертиниб", "osimertinib", "тагриссо", "tagrisso",
        "гефитиниб", "gefitinib", "ирресса", "iressa",
        "эрлотиниб", "erlotinib", "тарцева", "tarceva",
        "афатиниб", "afatinib", "жижак", "gilotrif",
        "дакомитиниб", "dacomitinib", "визимпро", "vizimpro"
    }

    # ALK-ингибиторы
    ALK_INHIBITORS = {
        "алектиниб", "alectinib", "алесенса", "alecensa",
        "кризотиниб", "crizotinib", "ксалкори", "xalkori",
        "бригатиниб", "brigatinib", "алунбриг", "alunbrig",
        "лорлатиниб", "lorlatinib", "лорбрена", "lorbrena"
    }

    # Иммунотерапия требует PD-L1
    IMMUNOTHERAPY_DRUGS = {
        "пембролизумаб", "pembrolizumab", "кейтруда", "keytruda",
        "ниволумаб", "nivolumab", "опдиво", "opdivo",
        "атезолизумаб", "atezolizumab", "тесентрик", "tecentriq",
        "дурвалумаб", "durvalumab", "имфинзи", "imfinzi",
        "авелумаб", "avelumab", "бавенсио", "bavencio"
    }

    # Противопоказания для иммунотерапии
    IMMUNOTHERAPY_CONTRAINDICATIONS = {
        "аутоиммунное заболевание", "autoimmune disease",
        "активный гепатит", "active hepatitis",
        "вич", "hiv", "вич-инфекция",
        "беременность", "pregnancy"
    }

    def __init__(self):
        self.issues_found: List[Issue] = []

    def validate(self, input_data: InputData, retrieved_chunks: list) -> List[Issue]:
        """
        Основной метод валидации. Проверяет:
        1. Соответствие линии терапии
        2. Наличие необходимых маркеров для таргетной терапии
        3. Противопоказания
        4. Дозировки и схемы (базовые проверки)
        """
        self.issues_found = []

        # Правило 1: Линия терапии
        self._validate_therapy_line(input_data, retrieved_chunks)

        # Правило 2: Маркеры для таргетной терапии
        self._validate_molecular_markers(input_data)

        # Правило 3: Противопоказания
        self._validate_contraindications(input_data)

        # Правило 4: Логические проверки схемы
        self._validate_regimen_logic(input_data)

        return self.issues_found

    def _validate_therapy_line(self, input_data: InputData, retrieved_chunks: list):
        """Проверка соответствия линии терапии в назначении и рекомендациях"""
        current_line = input_data.treatment.therapy_line

        for chunk in retrieved_chunks:
            metadata = chunk[1] if isinstance(chunk, tuple) else chunk.metadata
            chunk_line = metadata.get("therapy_line")

            # Если в чанке указана линия и она не совпадает — это проблема
            if chunk_line is not None and chunk_line != current_line:
                # Проверяем, не является ли это переходом на следующую линию
                if chunk_line > current_line:
                    self.issues_found.append(Issue(
                        code="PREMATURE_THERAPY_LINE",
                        severity="high",
                        title="Преждевременная смена линии терапии",
                        details=f"Назначена {current_line}-я линия, но рекомендация относится к {chunk_line}-й линии. Возможно, требуется провести дополнительные исследования или консилиум.",
                        suggested_action=f"Рассмотреть возможность перехода на {chunk_line}-ю линию согласно рекомендациям, если предыдущая терапия неэффективна.",
                        confidence=0.92
                    ))
                elif chunk_line < current_line:
                    self.issues_found.append(Issue(
                        code="REGIMEN_LINE_MISMATCH",
                        severity="medium",
                        title="Несоответствие линии терапии",
                        details=f"Назначена {current_line}-я линия, но препараты соответствуют {chunk_line}-й линии.",
                        suggested_action="Проверить корректность назначения или обновить рекомендации.",
                        confidence=0.85
                    ))
                break  # Достаточно одного несоответствия

    def _validate_molecular_markers(self, input_data: InputData):
        """Проверка наличия необходимых молекулярных маркеров"""
        regimen = [r.lower() for r in input_data.treatment.proposed_regimen]
        markers = input_data.molecular_markers

        # Проверка EGFR для ТКИ
        if self._contains_any_drug(regimen, self.EGFR_TKIS):
            if not markers or markers.EGFR not in ["positive", "mutated", "mutant", "да"]:
                self.issues_found.append(Issue(
                    code="MISSING_EGFR_FOR_TKI",
                    severity="critical",
                    title="Критическое: EGFR-мутация не подтверждена для ТКИ",
                    details="Назначена таргетная терапия (осимертиниб/гефитиниб/эрлотиниб) без подтвержденной EGFR-сенситизирующей мутации. Это противопоказание.",
                    suggested_action="Срочно провести тестирование EGFR (экзоны 18-21) или сменить схему на химиотерапию/иммунотерапию.",
                    confidence=0.98
                ))

        # Проверка ALK для ALK-ингибиторов
        if self._contains_any_drug(regimen, self.ALK_INHIBITORS):
            if not markers or markers.ALK not in ["positive", "rearranged", "fusion", "да"]:
                self.issues_found.append(Issue(
                    code="MISSING_ALK_FOR_INHIBITOR",
                    severity="critical",
                    title="Критическое: ALK-реаранжировка не подтверждена",
                    details="Назначен ALK-ингибитор без подтвержденной ALK-реаранжировки (FISH или IHC).",
                    suggested_action="Провести тестирование ALK (FISH) перед назначением или сменить терапию.",
                    confidence=0.97
                ))

        # Проверка PD-L1 для иммунотерапии (особенно важна для немелкоклеточного рака легкого)
        if self._contains_any_drug(regimen, self.IMMUNOTHERAPY_DRUGS):
            pd_l1_value = markers.PD_L1 if markers else None

            # Проверяем, есть ли значение PD-L1 и оно ли числовое
            has_pdl1 = pd_l1_value is not None and self._extract_percentage(pd_l1_value) is not None

            if not has_pdl1:
                # Для первой линии иммунотерапии PD-L1 обязателен
                if input_data.treatment.therapy_line == 1:
                    self.issues_found.append(Issue(
                        code="MISSING_PDL1_FIRST_LINE",
                        severity="high",
                        title="Отсутствует PD-L1 тестирование для первой линии",
                        details="Для назначения иммунотерапии (пембролизумаб/ниволумаб) в первой линии необходимо знать уровень PD-L1 (TPS).",
                        suggested_action="Провести IHC для PD-L1 (клон 22C3 или аналог). При PD-L1 ≥ 50% — пембролизумаб монотерапия возможна.",
                        confidence=0.94
                    ))
                else:
                    self.issues_found.append(Issue(
                        code="MISSING_PDL1_TEST",
                        severity="medium",
                        title="Рекомендуется PD-L1 тестирование",
                        details="Для иммунотерапии рекомендуется определение PD-L1 для оценки вероятности ответа.",
                        suggested_action="Провести PD-L1 IHC для документирования статуса.",
                        confidence=0.85
                    ))
            else:
                # Проверка уровня PD-L1 для конкретных препаратов
                pdl1_percent = self._extract_percentage(pd_l1_value)
                if pdl1_percent is not None and pdl1_percent < 1 and "пембролизумаб" in str(regimen):
                    self.issues_found.append(Issue(
                        code="LOW_PDL1_PEMBRO",
                        severity="medium",
                        title="Низкий PD-L1 для пембролизумаба",
                        details=f"PD-L1 = {pd_l1_value}. При PD-L1 < 1% монотерапия пембролизумабом в первой линии НМРЛ не рекомендуется (требуется комбинация с химиотерапией).",
                        suggested_action="Рассмотреть пембролизумаб + пеметрексед/платина или альтернативные схемы.",
                        confidence=0.88
                    ))

    def _validate_contraindications(self, input_data: InputData):
        """Проверка противопоказаний к назначенной терапии"""
        regimen = [r.lower() for r in input_data.treatment.proposed_regimen]
        context = input_data.patient_context

        # Проверка противопоказаний для иммунотерапии
        if self._contains_any_drug(regimen, self.IMMUNOTHERAPY_DRUGS):
            # Проверка сопутствующих заболеваний
            if context.comorbidities:
                comorb_lower = [c.lower() for c in context.comorbidities]

                for contraindication in self.IMMUNOTHERAPY_CONTRAINDICATIONS:
                    if any(contraindication in com for com in comorb_lower):
                        self.issues_found.append(Issue(
                            code="IMMUNOTHERAPY_CONTRAINDICATION",
                            severity="critical",
                            title="Противопоказание к иммунотерапии",
                            details=f"Обнаружено противопоказание: {contraindication}. Иммунотерапия может вызвать тяжелые аутоиммунные осложнения.",
                            suggested_action="Противопоказание абсолютное. Рассмотреть альтернативные схемы (химиотерапия, таргетная терапия при наличии мутаций).",
                            confidence=0.95
                        ))
                        break

            # Проверка возраста (иммунотерапия малоэффективна при очень низком возрасте — редко, но возможно)
            if context.age and context.age < 18:
                self.issues_found.append(Issue(
                    code="PEDIATRIC_IMMUNOTHERAPY",
                    severity="high",
                    title="Иммунотерапия в педиатрии",
                    details=f"Возраст {context.age} лет. Данные по иммунотерапии в педиатрической онкологии ограничены.",
                    suggested_action="Консилиум с педиатрическим онкологом. Возможно, требуется участие в клиническом исследовании.",
                    confidence=0.80
                ))

    def _validate_regimen_logic(self, input_data: InputData):
        """Логические проверки схемы лечения"""
        regimen = [r.lower() for r in input_data.treatment.proposed_regimen]

        # Проверка на дублирование механизмов (например, два ТКИ одновременно)
        egfr_count = sum(1 for drug in regimen if any(tki in drug for tki in self.EGFR_TKIS))
        if egfr_count > 1:
            self.issues_found.append(Issue(
                code="DUAL_TKI",
                severity="high",
                title="Двойная блокада EGFR",
                details="Назначено два EGFR-ТКИ одновременно. Это не рекомендуется из-за повышенной токсичности без доказанного преимущества.",
                suggested_action="Оставить один препарат (предпочтительно осимертиниб как 3-е поколение).",
                confidence=0.90
            ))

        # Проверка комбинации иммунотерапии + ТКИ (опасная комбинация — пневмонит)
        has_immuno = self._contains_any_drug(regimen, self.IMMUNOTHERAPY_DRUGS)
        has_tki = self._contains_any_drug(regimen, self.EGFR_TKIS)

        if has_immuno and has_tki:
            self.issues_found.append(Issue(
                code="IMMUNO_TKI_COMBINATION",
                severity="critical",
                title="Опасная комбинация: Иммунотерапия + ТКИ",
                details="Одновременное назначение иммунотерапии и EGFR-ТКИ противопоказано из-за высокого риска тяжелого пневмонита и гепатотоксичности.",
                suggested_action="Отменить одну из терапий. При EGFR+ предпочтение ТКИ, при EGFR- и PD-L1≥50% — иммунотерапия.",
                confidence=0.96
            ))

        # Проверка пустого назначения
        if not regimen:
            self.issues_found.append(Issue(
                code="EMPTY_REGIMEN",
                severity="high",
                title="Не указана схема лечения",
                details="Отсутствуют препараты в назначении.",
                suggested_action="Заполнить предполагаемую схему лечения.",
                confidence=1.0
            ))


    def _contains_any_drug(self, regimen: List[str], drug_set: Set[str]) -> bool:
        """Проверяет, содержит ли схема хотя бы один препарат из набора"""
        return any(drug in r for r in regimen for drug in drug_set)

    def _extract_percentage(self, value) -> Optional[float]:
        """Извлекает числовое значение процента из строки (например, '65%' → 65.0)"""
        if value is None:
            return None

        # Если уже число — возвращаем как есть
        if isinstance(value, (int, float)):
            return float(value)

        # Преобразуем в строку и убираем пробелы
        str_value = str(value).strip()

        # Удаляем знак процента если есть
        if str_value.endswith('%'):
            str_value = str_value[:-1].strip()

        # Пробуем преобразовать в число
        try:
            return float(str_value)
        except ValueError:
            pass

        # Если не получилось — ищем число в строке через regex
        match = re.search(r'(\d+(?:\.\d+)?)', str_value)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass

        return None


class ComorbidityChecker:
    """Дополнительный чекер для проверки сопутствующих заболеваний и взаимодействий"""

    # Препараты, требующие коррекции дозы при почечной недостаточности
    RENAL_ADJUSTMENT = {"цисплатин", "cisplatin", "пеметрексед", "pemetrexed", "метотрексат"}

    # Препараты, требующие коррекции при печеночной недостаточности
    HEPATIC_ADJUSTMENT = {"доцетаксел", "docetaxel", "паклитаксел", "paclitaxel", "винорельбин", "vinorelbine"}

    def check_organ_dysfunction(self, input_data: InputData) -> List[Issue]:
        """Проверка необходимости коррекции дозы при органной недостаточности"""
        issues = []
        regimen = [r.lower() for r in input_data.treatment.proposed_regimen]
        comorbidities = input_data.patient_context.comorbidities or []

        # Проверка почек
        if any("почки" in c.lower() or "renal" in c.lower() or "ckd" in c.lower() for c in comorbidities):
            for drug in self.RENAL_ADJUSTMENT:
                if any(drug in r for r in regimen):
                    issues.append(Issue(
                        code="RENAL_DOSE_ADJUSTMENT",
                        severity="high",
                        title="Требуется коррекция дозы при почечной недостаточности",
                        details=f"{drug} требует коррекции дозы при снижении СКФ. Риск нефротоксичности.",
                        suggested_action="Пересчитать дозу по СКФ (формула CKD-EPI). Контроль креатинина.",
                        confidence=0.90
                    ))

        return issues

