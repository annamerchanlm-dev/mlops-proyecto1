FROM python:3.12-slim-bookworm

# uv desde PyPI (raw.githubusercontent.com está bloqueado en la VM)
RUN pip install --no-cache-dir uv==0.12.19

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /srv

# Dependencias primero (capa cacheada): cambiar el código no reinstala nada
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app/ ./app/

# La API no necesita privilegios: corre con un usuario sin permisos de root
RUN useradd --uid 10001 --no-create-home api
USER api

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
