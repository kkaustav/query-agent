FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system build dependencies for database drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and seed database
COPY . .
RUN python seed_data.py

# Create empty secrets file so Streamlit doesn't raise StreamlitSecretNotFoundError
RUN mkdir -p .streamlit && touch .streamlit/secrets.toml

# Expose Streamlit default port
EXPOSE 8501

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Launch Streamlit with reverse-proxy flags
ENTRYPOINT ["streamlit", "run", "app.py", \
            "--server.port=8501", \
            "--server.address=0.0.0.0", \
            "--server.enableCORS=false", \
            "--server.enableXsrfProtection=false"]