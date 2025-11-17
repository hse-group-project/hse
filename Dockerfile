FROM python:3.11-slim

COPY pyproject.toml uv.lock /tmp/

RUN pip install uv

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-dev \
    && rm -rf /var/lib/apt/lists/*

RUN cd /tmp && uv sync --locked

COPY . /code
WORKDIR /code

CMD ["bash"]