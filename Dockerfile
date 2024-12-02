FROM python:3.10.15-bookworm

WORKDIR /app

COPY src/requirements.txt .
RUN pip install -r requirements.txt

COPY logs/consumer.log logs/consumer.log
COPY schemas/request-schema.json schemas/request-schema.json
COPY src/consumer.py .

CMD ["python", "consumer.py", "--region", "us-east-1", "-rq", "https://sqs.us-east-1.amazonaws.com/767843770882/cs5250-requests", "-dbt", "widgets"]