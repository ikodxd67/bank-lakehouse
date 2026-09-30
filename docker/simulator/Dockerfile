FROM python:3.12-slim
WORKDIR /opt/bank
COPY pyproject.toml ./
COPY src ./src
COPY sql ./sql
RUN pip install --no-cache-dir .
ENV BANK_PROJECT_DIR=/opt/bank PYTHONUNBUFFERED=1
CMD ["bank", "simulate"]
