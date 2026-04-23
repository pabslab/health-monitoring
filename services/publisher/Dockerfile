FROM python:3.11-slim

WORKDIR /app

COPY services/publisher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/config ./config
COPY src/core ./core
COPY src/datasources ./datasources
COPY src/publisher ./publisher
COPY services/publisher/main.py .

CMD ["python", "main.py"]
