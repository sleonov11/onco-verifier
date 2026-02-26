# Data

Папка содержит размеченные клинические рекомендации и тестовые сценарии для AI-помощника проверки лечения онкопациентов.

## Структура

```
data/
├── labeled/           # Размеченные JSON-файлы с клиническими правилами
├── test_cases/
│   ├── compliant/     # Тестовые истории болезни, соответствующие рекомендациям
│   └── non_compliant/ # Тестовые истории болезни с отклонениями от рекомендаций
└── README.md
```

---

## labeled/

Каждый файл соответствует одному гайдлайну и содержит массив правил.

**Источники:** клинические рекомендации RUSSCO, Минздрав РФ, ESMO, NCCN.  
**Нозологии в MVP:** немелкоклеточный рак лёгкого (C34).

### Формат файла

```json
{
  "source_file": "2025-1-1-04.pdf",
  "total_rules": 224,
  "rules": [ ... ]
}
```

### Формат одного правила

```json
{
  "source": {
    "guideline": "Немелкоклеточный рак легкого. Клинические рекомендации RUSSCO",
    "section": "3.1.1. Принципы лечения пациентов с 0-IIIА стадиями НМРЛ",
    "page": 100,
    "country": "ru"
  },
  "metadata": {
    "nosology": "Немелкоклеточный рак лёгкого",
    "icd10": "C34",
    "stage": "IB-IIIA",
    "tnm": null,
    "histology": "неплоскоклеточный",
    "molecular_markers": {
      "EGFR": "del19 или L858R"
    },
    "therapy_line": 1,
    "treatment_schema": "Осимертиниб",
    "evidence_level": "A"
  },
  "rule": {
    "condition": {
      "stage": "IB-IIIA",
      "molecular_markers": {
        "EGFR": "del19 или L858R"
      }
    },
    "recommendation": "При выявлении активирующих мутаций гена EGFR рекомендуется адъювантная терапия осимертинибом.",
    "recommendation_type": "preferred",
    "confidence": "high"
  },
  "text": {
    "doctor_version": "Текст для врача с клинической терминологией.",
    "patient_version": "Текст для пациента без медицинских терминов.",
    "original_guideline_text": "Дословная цитата из исходного гайдлайна."
  }
}
```

### Допустимые значения полей

| Поле | Допустимые значения |
|------|-------------------|
| `recommendation_type` | `preferred` \| `alternative` \| `not_recommended` |
| `confidence` | `high` \| `medium` \| `low` |
| `country` | `ru` \| `eu` \| `us` |
| `icd10` | Код МКБ-10, например `C34` |
| `therapy_line` | `1`, `2`, `3` или `null` |
| `evidence_level` | `A`, `B1`, `B2`, `C` или `null` |

### Правила заполнения

- Все поля обязательны. Если значение неизвестно — `null`.
- `condition` должен содержать минимальный набор параметров для однозначного матчинга с данными пациента.
- `molecular_markers` — только те маркеры, которые явно упомянуты в тексте гайдлайна.
- `patient_version` — без медицинских терминов, понятный неспециалисту.
- `original_guideline_text` — точная цитата, не перефраз.
- Статус лечения ("после операции", "до начала лечения") указывается в `condition.treatment_status`, а не внутри `molecular_markers`.
- Составные маркеры (`EGFR/ALK`) разбиваются на отдельные ключи: `"EGFR": "negative", "ALK": "negative"`.

---

## test_cases/

Синтетические истории болезни для валидации системы.

### compliant/

Случаи, в которых назначенное лечение **соответствует** рекомендациям. Система должна вернуть `is_compliant: true`.

### non_compliant/

Случаи, в которых назначенное лечение **не соответствует** рекомендациям. Система должна обнаружить конкретное несоответствие и вернуть `is_compliant: false` с описанием проблемы.

### Формат тест-кейса

```json
{
  "case_id": "nmrl_001",
  "description": "НМРЛ IV стадия, EGFR+, назначен осимертиниб — корректно",
  "expected_result": {
    "is_compliant": true
  },
  "input": {
    "role": "doctor",
    "locale": "ru",
    "input": {
      "diagnosis": {
        "name": "Немелкоклеточный рак лёгкого",
        "icd10": "C34",
        "stage": "IV",
        "tnm": "T2N2M1",
        "histology": "Аденокарцинома"
      },
      "molecular_markers": {
        "EGFR": "del19"
      },
      "patient_context": {
        "age": 58,
        "sex": "female",
        "comorbidities": [],
        "symptoms": ["кашель", "одышка"]
      },
      "treatment": {
        "therapy_line": 1,
        "proposed_regimen": ["Осимертиниб"],
        "cycle": "ежедневно",
        "notes": ""
      },
      "free_text": {
        "doctor_notes": "",
        "patient_message": ""
      }
    }
  }
}
```