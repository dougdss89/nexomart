FROM python:3.12-slim-bookworm

# gcc required to compile hll (HyperLogLog C extension) and other C-based packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
