FROM python:3.9.19-slim

workdir /app/truedata-feed

COPY requirements.txt .

run pip install --no-cache-dir -r requirements.txt

copy . /app/truedata-feed

expose 8000

cmd ["python", "main.py"]