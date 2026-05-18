FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md /app/
COPY services /app/services
RUN pip install --no-cache-dir -e ".[postgres,redis,crawler,admissions,jobs,news,research,institutions]"
CMD ["python", "-m", "services.lite_pipeline.main", "worker:run"]
