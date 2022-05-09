FROM golang:1.18 as builder

WORKDIR /srv

COPY . /srv

RUN apt-get update \
    && apt-get install -y librdkafka-dev libmagic-dev \
    && make


FROM debian:bullseye-slim

RUN apt-get update \
    && apt-get install -y librdkafka-dev libmagic-dev \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r downloader \
    && useradd --no-log-init --shel /bin/bash -r -g downloader downloader

COPY --chown=downloader:downloader --from=builder \
        /srv/downloader /srv/config.json.sample /srv/

USER downloader
WORKDIR /srv

ENTRYPOINT [ "/srv/downloader" ]
CMD [ "--help" ]
