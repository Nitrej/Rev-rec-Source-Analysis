# Use official Python image
FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Copy project files
COPY . /app

# Install system dependencies (minimal)
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Set environment variable to avoid Beam warnings
ENV PYTHONUNBUFFERED=1

