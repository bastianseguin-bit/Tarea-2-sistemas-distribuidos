FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY llm_worker.py .

ENV KAFKA_HOST=kafka:9092
ENV OLLAMA_HOST=http://ollama:11434

CMD ["python", "llm_worker.py"]