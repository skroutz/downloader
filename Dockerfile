FROM golang:1.18 as builder

WORKDIR /srv

COPY . /srv

RUN apt-get update \
    && apt-get install -y librdkafka-dev libmagic-dev \
    && make

FROM debian:bullseye-slim

RUN apt-get update \
    && apt-get install -y librdkafka-dev libmagic-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r downloader \
    && useradd --no-log-init --shel /bin/bash -r -g downloader downloader

COPY --chown=downloader:downloader --from=builder /srv/downloader /srv/bin/entrypoint.sh /srv/
COPY --chown=downloader:downloader --from=builder /srv/config.test.json /srv/config.json

USER downloader
WORKDIR /srv

ENTRYPOINT [ "/srv/entrypoint.sh" ]
CMD [ "--help" ]
