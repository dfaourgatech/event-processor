FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
COPY producer ./producer
COPY schemas ./schemas

RUN pip install --no-cache-dir -e .

CMD ["python", "-m", "event_processor.main"]
