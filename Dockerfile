FROM python:3.11

WORKDIR /app

COPY backend/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ .

EXPOSE 7860

CMD ["bash", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-7860}"]