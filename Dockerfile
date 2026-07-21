FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyrogram TgCrypto motor pymongo python-dotenv httpx psutil
COPY . .
CMD ["python", "bot.py"]
