FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias del sistema (certs, locales, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Puerto de la API (configurable)
ENV API_HOST=0.0.0.0 \
    API_PORT=9009 \
    API_WORKERS=2 \
    TIME_ZONE=America/Mexico_City

# Variables de conexión a Trino/Nessie (puedes sobreescribirlas en compose)
ENV TRINO_HOST=trino \
    TRINO_PORT=8080 \
    TRINO_USER=api \
    TRINO_CATALOG=nessie \
    TRINO_SCHEMA=telematics \
    API_TOKENS="token1,token2,token3"

CMD ["sh", "-c", "uvicorn app.main:app --host ${API_HOST} --port ${API_PORT} --workers ${API_WORKERS}"]