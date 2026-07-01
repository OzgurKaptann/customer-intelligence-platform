# FastAPI REST layer for the Customer Intelligence Platform.
# Serves marts.* and ml.* outputs. Build context is ./api (see docker-compose.yml).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Pinned runtime dependencies (Requirement 11, NFR-6.1).
RUN pip install --no-cache-dir \
        fastapi==0.111.0 \
        "uvicorn[standard]==0.30.1" \
        asyncpg==0.29.0 \
        pydantic==2.7.4 \
        python-dotenv==1.0.1

# Copy the api/ source into the image.
COPY . .

EXPOSE 8000

ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
