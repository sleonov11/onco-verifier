# src/config.py — ФИНАЛЬНАЯ ВЕРСИЯ
import os
from dotenv import load_dotenv
from loguru import logger
import sys
import torch

load_dotenv()

HAS_GPU = torch.cuda.is_available()
GPU_NAME = torch.cuda.get_device_name(0) if HAS_GPU else "None"

logger.remove()
logger.add(sys.stdout, level=os.getenv("LOG_LEVEL", "INFO"), 
           format="{time:HH:mm:ss} | {level} | {message}")
logger.add("logs/ml_service.log", rotation="10 MB", level="DEBUG", compression="zip")

logger.info(f"GPU: {GPU_NAME}" if HAS_GPU else "GPU: not available, using CPU")

GIGACHAT_API_KEY = os.getenv("GIGACHAT_API_KEY", "")

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "mock")
if LLM_PROVIDER == "gigachat" and not GIGACHAT_API_KEY:
    logger.warning("GigaChat selected but no API key, using mock")
    LLM_PROVIDER = "mock"

#Пути
CHROMA_PERSIST_DIR = os.getenv("CHROMA_PERSIST_DIR", "./indices/chroma_db")
MODELS_CACHE_DIR = os.getenv("MODELS_CACHE_DIR", "./models_cache")

#GPU оптимизации
USE_GPU_RERANKER = os.getenv("USE_GPU_RERANKER", "true").lower() == "true" and HAS_GPU
RERANKER_BATCH_SIZE = int(os.getenv("RERANKER_BATCH_SIZE", "16" if HAS_GPU else "4"))
RERANKER_CANDIDATES = int(os.getenv("RERANKER_CANDIDATES", "15" if HAS_GPU else "8"))

#Функционал
USE_METADATA_FILTER = os.getenv("USE_METADATA_FILTER", "true").lower() == "true"
USE_QUERY_EXPANSION = os.getenv("USE_QUERY_EXPANSION", "true").lower() == "true"