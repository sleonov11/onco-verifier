```
uv venv --python 3.11
.venv\Scripts\activate  # On Windows
source .venv/bin/activate  # On Unix
uv pip install -e .
uv sync
```

чтобы запустить код на gpu:
```
uv pip uninstall torch -y
uv pip install torch==2.3.0 --index-url https://download.pytorch.org/whl/cu121
```

Run: uvicorn api:app --reload (localhost:8000/docs для Swagger)


Requirements

Python 3.11 (pinned for compatibility).
GPU recommended (NVIDIA with CUDA 11.8+ for torch).
Dependencies managed via pyproject.toml (uv/poetry compatible).

Index data (initial setup):
```
uv run python -m scripts.index_all  # Or index_data.py --chunks data/chunks.jsonl
```

## Running the Service
Start the FastAPI server:  
```
uv run uvicorn src.api:app --host 0.0.0.0 --port 8000
```

- Access: http://localhost:8000/docs (Swagger UI for interactive API testing).
- Root: http://localhost:8000/ — Service info.

## API Endpoints
- **POST /check**: Main endpoint for treatment verification.  
  Input: CheckRequest JSON (see schemas.py: diagnosis, markers, treatment).  
  Output: CheckResponse (is_compliant, issues, explanations, sources).  
  Example curl:  
  ```
  curl -X POST http://localhost:8000/check -H "Content-Type: application/json" -d '{"request_id": "test1", "role": "doctor", "locale": "ru", "input": {...}}'
  ```

- **POST /check/batch**: Batch processing for multiple requests.  
  Input: List of CheckRequest.  
  Output: Processed results + errors.

- **GET /health**: Service health check (components status, config).  
  Example: {"status": "ok", "components": {...}}.

- **GET /stats**: Monitoring stats (cache hits, config).  

- **Error Handling**: Global handler for 500 errors (logs exceptions).

CORS enabled for all origins (*) — ready for frontend integration.

## Integration with Frontend
- **API Calls:** From frontend (e.g., React/Axios): POST to /check with JSON payload. Handle response (is_compliant, explanations).
- **Auth/Security:** No auth yet — add JWT if needed (FastAPI dependency).
- **Deployment:** Dockerize (add Dockerfile: FROM python:3.11, COPY ., uv pip install -e .). Run on server/EC2 with GPU.
- **Monitoring:** Logs in logs/ml_service.log. Use /health for uptime checks.
- **Scaling:** Batch endpoint for multiple patients. GigaChat rate limits — monitor LLM usage.

## Troubleshooting
- NumPy/Torch errors: Ensure pins in pyproject.toml (numpy<2, transformers=4.42.0).
- No GPU: Set USE_GPU_RERANKER=false in .env.
- Indexing fails: Check data/chunks.jsonl format.
- LLM issues: Fallback to "mock" if key invalid.


