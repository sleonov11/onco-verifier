# syntax=docker/dockerfile:1.6
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN pip install --no-cache-dir uv

# Файлы для установки зависимостей и сборки editable-пакета
COPY pyproject.toml ./
COPY uv.lock* ./
# ВАЖНО: hatchling требует readme, если он указан в pyproject.toml
COPY README.md ./
# если есть, тоже можно:
# COPY LICENSE* ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev || uv sync --no-dev

ARG INSTALL_TORCH_CUDA121=0
RUN --mount=type=cache,target=/root/.cache/uv \
    if [ "$INSTALL_TORCH_CUDA121" = "1" ]; then \
      uv pip uninstall torch -y && \
      uv pip install torch==2.3.0 --index-url https://download.pytorch.org/whl/cu121 ; \
    fi

# Теперь копируем весь код
COPY . /app

EXPOSE 8000
CMD ["uv", "run", "uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
