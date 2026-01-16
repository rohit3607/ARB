FROM python:3.10-slim

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt update && \
    apt install -y --no-install-recommends \
        ffmpeg \
        mediainfo \
        ca-certificates \
        gcc \
        libffi-dev \
        libssl-dev \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

# Upgrade pip and install python deps
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir pyrogram==2.0.80 && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python", "bot.py"]