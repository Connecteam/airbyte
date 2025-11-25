FROM python:3.9-slim-bookworm as base

RUN apt-get update && apt-get upgrade -y \
    && pip install --upgrade pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /home/octavia-cli
COPY . ./

RUN pip install --no-cache-dir .

RUN useradd --create-home --shell /bin/bash octavia-cli
USER octavia-cli

WORKDIR /home/octavia-project
ENTRYPOINT ["octavia"]

LABEL io.airbyte.version=0.50.0
LABEL io.airbyte.name=airbyte/octavia-cli
