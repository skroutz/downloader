# Changelog

Breaking changes are prefixed with a "[BREAKING]" label.

## master (unreleased)

### Changed

- [BREAKING] Downloaded files will now be deleted from the disk after 3 hours,
  if the job's notification was successfully delivered. Furthermore, responding
  to an HTTP callback with 201 is now the same as with 200. [[#5](https://github.com/skroutz/downloader/pull/5)]

### Added

- Support pluggable notification backends. [[#5](https://github.com/skroutz/downloader/pull/5)]
- Add a Kafka notification backend implementation. librdkafka is now required
  to compile downloader. It is also a runtime requirement if the Kafka backend
  is enabled. [[#5](https://github.com/skroutz/downloader/pull/5)]

## 0.0.1 (2019-04-08)

This is the first public release that includes an HTTP API for scheduling
jobs, downloading files with rate-limiting features and a mechanism for
getting notified when a job is complete via an HTTP callback request.
