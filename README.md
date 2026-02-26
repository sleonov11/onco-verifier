# Onco AI Project

🔗 **Prototype:**  
sechenovskiy.vercel.app

---

## 🧠 О проекте

**Onco AI Project** — интеллектуальный AI-ассистент для проверки назначений онкологических пациентов на соответствие клиническим рекомендациям.

Система:

- Анализирует диагноз и план лечения
- Проверяет соответствие российским и международным рекомендациям
- Объясняет результат в двух режимах:
    - 👨‍⚕️ Doctor mode — подробный профессиональный разбор
    - 🧑‍⚕️ Patient mode — понятное объяснение простым языком
- Поддерживает диалоговый режим уточняющих вопросов
- Использует Retrieval + Reranking pipeline

---

## 🏗 Архитектура

Frontend (React + Ant Design)  
↓  
ExpressJS Gateway  
↓  
FastAPI AI Service  
↓  
Giga Chat

---

## ⚙️ Технологии

- **Frontend:** React + Vite + MobX + Ant Design
- **Backend Gateway:** ExpressJS (TypeScript)
- **AI Core:** FastAPI + PyTorch + Sentence Transformers
- **Vector DB:** Chroma
- **Embeddings:** multilingual-e5-large-instruct
- **Reranker:** bge-reranker-v2-m3
- **Containerization:** Docker + Docker Compose

---

# 🚀 Запуск проекта

## 1. Docker

```bash
  docker-compose up -d --build
```

## 2. Запуск индексирования
```bash
  docker-compose run --rm fastapi uv run python -m scripts.index_all 
```