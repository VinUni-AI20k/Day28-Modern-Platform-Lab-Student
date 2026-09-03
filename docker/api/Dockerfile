FROM python:3.12-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --default-timeout=180 ".[retrieval,ml]"

EXPOSE 8000
CMD ["lab28", "serve", "--host", "0.0.0.0", "--port", "8000"]
