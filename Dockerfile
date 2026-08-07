FROM python:3.11-trixie
#UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root
WORKDIR /root/

# Disable development dependencies
ENV UV_NO_DEV=1

# basic utility packages
RUN apt-get update && apt-get install -yq --no-upgrade unzip curl

# environment
COPY . .
RUN uv sync --locked

# version tracking
ARG githash
ENV GITHASH=$githash

# Run our flow script when the container starts
CMD uv run python -m ingestion_consumer.ingestion_process
