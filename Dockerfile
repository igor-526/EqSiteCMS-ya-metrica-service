FROM python:3.14.6-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN pip install --no-cache-dir uv==0.8.15

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev

COPY src ./src

CMD ["uv", "run", "--no-sync", "uvicorn", "main:app", "--app-dir", "src", "--host", "0.0.0.0", "--port", "8000"]
