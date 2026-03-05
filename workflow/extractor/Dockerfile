FROM python:3.10-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_311_to_gcs.py .
COPY increment_311_to_gcs.py .

ENTRYPOINT ["python"]