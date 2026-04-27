# Start with slim Python 3.13 image
FROM python:3.13.10-slim

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# --- NEW: Install system dependencies ---
# libeccodes-dev is required for the cfgrib engine to work
RUN apt-get update && apt-get install -y --no-install-recommends \
    libeccodes-dev \
    && rm -rf /var/lib/apt/lists/*
# ---------------------------------------

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY "pyproject.toml" "uv.lock" ".python-version" ./
RUN uv sync --locked

COPY ghcnd_ingestor.py .

ENTRYPOINT ["uv", "run", "python", "ghcnd_ingestor.py"]