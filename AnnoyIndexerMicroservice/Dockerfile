FROM python:3.8-slim

RUN apt-get update && \
    apt-get install -y gcc g++ make

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends apt-utils

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn

EXPOSE 8080

# Define environment variable
ENV FLASK_APP=application.py
ENV FLASK_ENV=production
ENV GOOGLE_APPLICATION_CREDENTIALS="./solidmetaprediction-a9d846238df8.json"

CMD exec gunicorn --bind :$PORT --workers 2 --threads 4 --timeout 0 application:application