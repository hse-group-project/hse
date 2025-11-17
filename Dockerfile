FROM public.ecr.aws/docker/library/python:3.11-slim-bullseye

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

# Копирование только файлов зависимостей сначала
COPY pyproject.toml uv.lock* /code/
WORKDIR /code

# Установка зависимостей
RUN uv sync --locked --no-dev

# Копирование остального кода
COPY . /code

CMD ["bash"]