FROM public.ecr.aws/docker/library/python:3.11-alpine

RUN apk update && apk add --no-cache \
    build-base \
    postgresql-dev \
    linux-headers

RUN pip install uv

COPY . /code
WORKDIR /code

RUN uv sync --locked

CMD ["bash"]