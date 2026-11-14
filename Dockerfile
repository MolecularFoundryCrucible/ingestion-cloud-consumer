FROM python:3.11-trixie

COPY --from=ghcr.io/astral-sh/uv:0.11.16 /uv /uvx /bin/

ENV UV_NO_DEV=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    MPLCONFIGDIR=/tmp/matplotlib \
    XDG_CACHE_HOME=/tmp/cache

RUN apt-get update && apt-get install -yq --no-upgrade unzip curl && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 worker && mkdir /app && chown worker:worker /app
USER worker
WORKDIR /app

COPY --chown=worker:worker pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-install-project

COPY --chown=worker:worker . .
RUN uv sync --locked

ARG CRUCIBLE_INGESTION_SPEC=""
RUN if [ -n "$CRUCIBLE_INGESTION_SPEC" ]; then uv pip install --python /app/.venv/bin/python "$CRUCIBLE_INGESTION_SPEC"; fi

# hyperspy writes its config file on first import; create it now so a read-only root filesystem works.
RUN /app/.venv/bin/python -c "import hyperspy.api" && test -f /home/worker/.hyperspy/hyperspyrc

CMD ["/app/.venv/bin/python", "-m", "crucible_ingestion_worker"]
