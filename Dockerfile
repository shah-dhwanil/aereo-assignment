# Bulk certificate generator — multi-stage image on uv's Python 3.14 slim base.
#
# Build stage resolves + installs locked deps into /app/.venv.
# Runtime stage copies the venv plus source, fonts, and migrations,
# then runs as a non-root user.

FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Metadata first for layer caching. README.md must be copied because
# pyproject.toml declares it as the package readme; src/ is needed so the
# project itself builds into the venv.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable


FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS runtime

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

COPY --from=builder /app/.venv /app/.venv
COPY src ./src
COPY assets ./assets
COPY alembic ./alembic
COPY alembic.ini ./

RUN useradd --create-home --uid 10001 --user-group appuser \
    && mkdir -p /app/storage \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000
