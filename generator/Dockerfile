# One-shot synthetic data generator for the Customer Intelligence Platform.
# Seeds raw.* tables via psycopg2, then exits. Build context is ./generator (see docker-compose.yml).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Pinned runtime dependencies (Requirement 1, NFR-6.1).
RUN pip install --no-cache-dir \
        Faker==25.9.2 \
        psycopg2-binary==2.9.9 \
        python-dotenv==1.0.1

# Copy the generator/ source into the image.
COPY . .

ENTRYPOINT ["python", "main.py"]
