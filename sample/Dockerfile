FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    SAMPLE_STATE_DIR=/tmp/sample-state

WORKDIR /srv

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY sample ./sample

EXPOSE 8001
CMD ["uvicorn", "sample_app:app", "--app-dir", "/srv/sample", "--host", "0.0.0.0", "--port", "8001"]
