# syntax=docker/dockerfile:1
#
# One image for every part of the pipeline; docker-compose.yml picks the command.

FROM python:3.12-slim AS build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
RUN pip install --no-cache-dir uv==0.11.32
WORKDIR /app

# dependencies first, so code changes don't invalidate these layers
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev --no-editable


FROM python:3.12-slim
RUN useradd --create-home --uid 1000 tramvaj
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY site ./site
RUN mkdir -p /app/state /app/samples && chown tramvaj /app/state /app/samples
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    TRAMVAJ_SITE=/app/site \
    STATE_DIR=/app/state
USER tramvaj
ENTRYPOINT ["tramvaj"]
CMD ["serve"]
