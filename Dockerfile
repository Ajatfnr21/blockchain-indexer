FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY indexer/ ./indexer/
COPY tests/ ./tests/

VOLUME ["/app/data"]

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "indexer.main:app", "--host", "0.0.0.0", "--port", "8000"]
