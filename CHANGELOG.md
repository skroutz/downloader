# Changelog

Breaking changes are prefixed with a "[BREAKING]" label.

## master (unreleased)



## 0.1.0 (2019-04-30)

This is a backwards-incompatible release with a bunch of new features (most
notably, a Kafka notification adapter).

### Changed

- [BREAKING] Downloaded files will now be deleted from the disk after 3 hours
  (configured via `deletion_interval`),
  if the job's notification was successfully delivered. Furthermore, responding
  to an HTTP callback with 201 has no longer any additional effect (it's the
  same as 200). [[#5](https://github.com/skroutz/downloader/pull/5)]

### Added

- Support for clients setting the User-Agent header (via `user_agent`) to use
  when downloading files. [[#14](https://github.com/skroutz/downloader/issues/14)]
- Support for clients setting a timeout (via `download_timeout`) for download
  requests. [[#14](https://github.com/skroutz/downloader/issues/14)]
- Support for clients setting an HTTP proxy (via `aggr_proxy`) to use
  when downloading files. [[#14](https://github.com/skroutz/downloader/issues/14)]
- Notifier metrics are now displayed in the web view. [[#11](https://github.com/skroutz/downloader/issues/11)]
- Support pluggable notification backends. [[#5](https://github.com/skroutz/downloader/pull/5)]
- Add a Kafka notification backend implementation. librdkafka is now required
  to compile downloader. It is also a runtime requirement if the Kafka backend
  is enabled. [[#5](https://github.com/skroutz/downloader/pull/5)]



## 0.0.1 (2019-04-08)

This is the first public release that includes an HTTP API for scheduling
jobs, downloading files with rate-limiting features and a mechanism for
getting notified when a job is complete via an HTTP callback request.
