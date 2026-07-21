FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pyrogram TgCrypto motor pymongo python-dotenv httpx psutil
COPY . .
CMD ["python", "bot.py"]
