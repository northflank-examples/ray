FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    RAY_USAGE_STATS_ENABLED=0 \
    RAY_enable_autoscaler_v2=0

WORKDIR /opt/ray-northflank
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 1000 ray

USER ray
WORKDIR /home/ray
ENTRYPOINT ["ray-northflank-start"]
CMD []
