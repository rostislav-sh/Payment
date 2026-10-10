# syntax=docker/dockerfile:1
# One image for every Python service (api, relay, consumer, migrations, webhook-mock);
# docker-compose.yml only changes the command.
#
#   docker build --target test .     # build + run the unit tests (fails on a failing test)
#   docker build .                   # runtime image (the test stage is not built)

ARG PYTHON_IMAGE=python:3.14-slim-trixie

# --- builder: dependencies and the project in /app/.venv ------------------------------------
FROM ${PYTHON_IMAGE} AS builder

COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /uvx /bin/

# Bytecode compiled at build time (faster start); copy instead of hardlinks (the cache mount
# is another filesystem); use the image's Python, never download one.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Dependencies only: this layer is rebuilt only when pyproject.toml / uv.lock change.
# --frozen here (uv cannot validate the lock without the project), --locked below.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project

COPY pyproject.toml uv.lock README.md alembic.ini ./
COPY src ./src
COPY migrations ./migrations
# --locked: fail if uv.lock does not match pyproject.toml (the lock is updated only by `uv lock`).
# --no-editable: the app package goes into site-packages (byte-compiled), so the runtime image
# needs only the venv, not the sources.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

# --- test: dev dependencies + unit tests (integration tests need Docker: run them on the host)
FROM builder AS test

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked
COPY tests ./tests
RUN uv run --no-sync pytest tests/unit -q

# --- runtime: no uv, no build tools, non-root -----------------------------------------------
FROM ${PYTHON_IMAGE} AS runtime

RUN useradd --system --no-create-home --uid 1000 app

WORKDIR /app
# Same base image and the same /app path as the builder: the venv contains absolute paths.
# Files stay owned by root: the app user can read but not modify code or packages.
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/alembic.ini ./
COPY --from=builder /app/migrations ./migrations

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER app
EXPOSE 8000

# Exec form: the Python process is PID 1's direct child (compose `init: true`) and receives
# SIGTERM itself, so graceful shutdown works.
# --timeout-graceful-shutdown < docker's 10 s stop timeout: in-flight requests may finish.
CMD ["uvicorn", "app.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", \
     "--timeout-graceful-shutdown", "8"]
