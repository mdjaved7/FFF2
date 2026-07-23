FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pyrogram tgcrypto motor psutil python-dotenv

COPY bot.py .

CMD ["python", "bot.py"]
