FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for volume mounting
RUN mkdir -p /app

# Set Python path to include the app directory
ENV PYTHONPATH=/app

CMD ["bash"]