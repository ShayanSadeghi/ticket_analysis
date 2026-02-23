FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/var/lib/dagster

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./src ./src
COPY ./analytics ./analytics

ENV PYTHONPATH=/app

EXPOSE 3000
CMD ["dagster", "webserver", "-h", "0.0.0.0", "-p", "3000"]