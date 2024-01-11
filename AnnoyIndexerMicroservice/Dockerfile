FROM python:3.8-slim

RUN apt-get update && \
    apt-get install -y gcc g++ make

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn

EXPOSE 5000

# Define environment variable
ENV FLASK_APP=application.py
ENV FLASK_ENV=production
ENV GOOGLE_APPLICATION_CREDENTIALS="./solidmetaprediction-a9d846238df8.json"

CMD ["sh", "-c", "gunicorn -c gunicorn_config.py application:application"]