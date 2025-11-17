FROM public.ecr.aws/docker/library/python:3.11-alpine

COPY . /code
WORKDIR /code

RUN pip install uv

RUN apk update && apk add --no-cache \
    build-base \
    postgresql-dev \
    linux-headers

RUN uv sync --locked

CMD ["bash"]