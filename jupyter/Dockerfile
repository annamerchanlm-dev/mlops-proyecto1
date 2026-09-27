FROM python:3.12-slim-bookworm

# uv se instala desde PyPI: raw.githubusercontent.com está bloqueado en la VM
# y ghcr.io no está garantizado. PyPI sí es alcanzable.
RUN pip install --no-cache-dir uv==0.12.19

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    HOME=/home/work \
    JUPYTER_RUNTIME_DIR=/tmp/jupyter-runtime

WORKDIR /app

# Dependencias primero (capa cacheada): cambiar notebooks no reinstala nada
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# HOME escribible para cualquier UID (el contenedor corre con el UID del usuario de la VM)
RUN mkdir -p /home/work/notebooks && chmod -R 1777 /home/work

WORKDIR /home/work/notebooks
EXPOSE 8888
