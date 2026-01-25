FROM python:3.12-slim

ENV PYTHONUNBUFFERED=True

# Set environment variables to suppress debconf warnings
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true

# Install Redis server
RUN apt-get update && apt-get install -y redis-server && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV APP_HOME=/app
WORKDIR $APP_HOME
COPY . ./

ENV PORT=8080
# Set the environment variable to tell Google's libraries where to find the key
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/app_secrets/parabolic-clock-457218-f5-886e8be4f217.json

RUN pip install --no-cache-dir -r requirements.txt

# Create startup script that starts Redis then the app
RUN echo '#!/bin/bash\necho "Starting Redis..."\nredis-server --daemonize yes --bind 127.0.0.1 --port 6379\necho "Redis started, starting FastAPI app..."\nexec uvicorn app:app --host 0.0.0.0 --port ${PORT}' > start.sh
RUN chmod +x start.sh

CMD ["./start.sh"]