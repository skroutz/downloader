FROM golang:1.18 as builder

WORKDIR /srv

COPY . /srv

RUN apt-get update \
    && apt-get install -y librdkafka-dev libmagic-dev \
    && make

FROM debian:bullseye-slim

RUN apt-get update \
    && apt-get install -y librdkafka-dev libmagic-dev ca-certificates supervisor \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r downloader \
    && useradd --no-log-init --shel /bin/bash -r -g downloader downloader

COPY --chown=downloader:downloader --from=builder /srv/downloader /srv/
COPY --chown=downloader:downloader --from=builder /srv/config.test.json /srv/config.json

COPY contrib/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
RUN mkdir -p /var/run/supervisor/ && chown downloader: /var/run/supervisor/

USER downloader
WORKDIR /srv

CMD [ "/usr/bin/supervisord" ]
