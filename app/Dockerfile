FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl git unzip build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install matplotlib

COPY . .

CMD ["python", "LLM.py"]