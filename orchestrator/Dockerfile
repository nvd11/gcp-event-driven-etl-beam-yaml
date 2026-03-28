FROM python:3.12-slim

# Install system dependencies if required by native extensions for gRPC / Beam
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies. This might take a while because apache-beam[gcp] is large.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY main.py .

# Use PORT environment variable provided by Cloud Run, default to 8080
ENV PORT=8080

# Run the web service using Uvicorn
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
