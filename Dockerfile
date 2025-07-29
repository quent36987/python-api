FROM python:3.12-bookworm

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

ADD . /app

ENV PORT=8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]