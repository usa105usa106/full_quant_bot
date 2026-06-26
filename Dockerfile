FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data

ENV PYTHONUNBUFFERED=1
ENV DB_PATH=/data/signals.db
ENV PORT=8080

CMD ["python", "bot.py"]
