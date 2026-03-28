FROM python:3.12-slim

# Install system dependencies if required by native extensions for gRPC / Beam
# Also install Java (default-jre-headless) because Apache Beam YAML WriteToBigQuery requires cross-language Java provider
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (usually /usr/lib/jvm/default-java on Debian-based slim images)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

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
