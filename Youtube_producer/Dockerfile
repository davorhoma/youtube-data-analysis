FROM python:3.11-slim

WORKDIR /app

RUN pip install pandas confluent-kafka

COPY producer.py .
COPY youtube_realtime_data.csv .

CMD ["python", "producer.py"]