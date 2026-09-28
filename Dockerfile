# Toolbox: everything around the stack (tests, shopper traffic, screenshots) with only Docker installed.
# No local Python, virtualenv or browser needed. With the stack running (docker compose up -d --build):
#
#   docker compose run --rm tests                     # the full test suite, incl. live integration tests
#   docker compose run --rm demo                      # 4 min of shopper traffic + screenshots
#   docker compose run --rm demo --no-screenshots     # traffic only
#
# The stack's own services have their own Dockerfiles (shop/, dashboard/, flink/, generator/, iceberg-rest/).
# Debian 12 (bookworm): the newest Debian that Playwright 1.49 supports for `install --with-deps`.
FROM python:3.12-slim-bookworm

WORKDIR /work
COPY requirements-dev.txt .
# Chromium + its system libraries for the browser tests and screenshots, and an emoji font so the
# store's emoji "product photos" render in screenshots.
RUN pip install --no-cache-dir -r requirements-dev.txt \
 && python -m playwright install --with-deps chromium \
 && apt-get install -y --no-install-recommends fonts-noto-color-emoji \
 && rm -rf /var/lib/apt/lists/*

COPY . .
ENV E2E_BROWSER=chromium \
    DEMO_BROWSER=chromium \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
CMD ["python", "-m", "pytest", "-rs"]
