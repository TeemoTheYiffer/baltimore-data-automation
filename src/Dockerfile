FROM python:3.10-slim

ENV PYTHONUNBUFFERED=True

ENV APP_HOME=/app
WORKDIR $APP_HOME
COPY . ./

ENV PORT=8080
# Set the environment variable to tell Google's libraries where to find the key
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/app_secrets/parabolic-clock-457218-f5-886e8be4f217.json

RUN pip install --no-cache-dir -r requirements.txt

CMD exec uvicorn app:app --host 0.0.0.0 --port ${PORT}