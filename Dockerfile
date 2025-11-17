FROM public.ecr.aws/docker/library/python:3.11-alpine

# Установка системных зависимостей
RUN apk update && apk add --no-cache \
    build-base \
    postgresql-dev \
    linux-headers \
    && rm -rf /var/cache/apk/*

# Установка uv
RUN pip install --no-cache-dir uv

# Копирование только файлов зависимостей сначала
COPY pyproject.toml uv.lock* /code/
WORKDIR /code

# Установка зависимостей (используется кэш Docker)
RUN uv sync --locked --no-dev

# Копирование остального кода
COPY . /code

CMD ["bash"]