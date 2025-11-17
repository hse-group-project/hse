FROM public.ecr.aws/docker/library/python:3.11-alpine

COPY pyproject.toml uv.lock /tmp/

RUN pip install uv

RUN apk update && apk add --no-cache \
    build-base \
    postgresql-dev \
    linux-headers

RUN cd /tmp && uv sync --locked

COPY . /code
WORKDIR /code

CMD ["bash"]