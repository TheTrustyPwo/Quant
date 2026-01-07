FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DUCKDB_PATH=/app/data/market_data.duckdb
ENV VISION_DOWNLOAD_DIR=/app/data/vision

RUN mkdir -p /app/data

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
