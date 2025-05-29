# Stage 1: Installing uv and the project dependencies
FROM python:3.10-slim-bullseye AS builder
WORKDIR /app
RUN python -m venv .venv
ENV PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync

# Stage 2: Final image with Chrome only (no ChromeDriver)
FROM python:3.10-slim-bullseye AS celery_setup

COPY --from=builder /app/.venv /app/.venv

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget curl unzip jq gnupg ca-certificates fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 libnss3 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 \
    xdg-utils libgbm1 libu2f-udev libvulkan1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY --chown=celery_user:celery_group . /app
WORKDIR /app
# Set environment
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app:/app/.venv/lib/python3.10/site-packages"

# Téléchargement automatique de la dernière version stable de Chrome
RUN CHROME_VERSION=$(curl -s https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json \
    | jq -r '.channels.Stable.version') \
    && echo "Using Chrome version: $CHROME_VERSION" \
    && wget -q https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chrome-linux64.zip \
    && unzip chrome-linux64.zip -d /app/data_extraction/Websites \
    && chmod +x /app/data_extraction/Websites/chrome-linux64/chrome \
    && rm chrome-linux64.zip

# Optional: Set Chrome binary path for undetected_chromedriver or Selenium
ENV CHROME_BIN=/app/data_extraction/Websites/chrome-linux64/chrome

# Create user for Celery
RUN groupadd --gid 1000 celery_group \
    && useradd --uid 1000 --gid 1000 -m celery_user

# Create logs folder and set permissions
RUN mkdir -p /app/data_extraction/Websites/log

WORKDIR /app
