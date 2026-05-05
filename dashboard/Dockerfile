FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --compile -r requirements.txt

COPY dashboard.py .

RUN adduser --disabled-password --no-create-home appuser
USER appuser

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "dashboard.py", \
            "--server.port=8501", \
            "--server.address=0.0.0.0", \
            "--server.headless=true"]
