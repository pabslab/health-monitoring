FROM python:3.11-slim

WORKDIR /app

COPY services/bridge/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/config ./config
COPY src/core ./core
COPY src/bridge ./bridge
COPY services/bridge/main.py .

CMD ["python", "main.py"]