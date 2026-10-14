FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Raw data is mounted at run time, never baked into the image:
#   docker run --rm -v "$PWD/data:/app/data" -v "$PWD/reports:/app/reports" <image> \
#       python scripts/run_pipeline.py all
CMD ["python", "scripts/run_pipeline.py", "audit"]
