# Glossary

This file contains common terms of downloader that are seen throughout the
codebase or the documentation.

- `job`: a `job` is essentially a task for a given `resource` download. When an incoming download request is received, a new `job` is created and queued. Eventually the job will be processed and the actual resource will be downloaded.

- `worker`: a worker is the component that performs the actual `jobs`. They work continuously, processing one `job` after another. Workers may be created/destroyed at will and typically there will be a fixed amount of workers for a variable amount of jobs (N:M where N=amount-of-workers and M=amount-of-jobs). The worker is the actual component that executes the HTTP requests to download the resource.

- `aggregation`: An `aggregation` consists of an `aggregation-id` and a `limit`. The `aggregation` is the concept through which the rate limit rules are defined and enforced. When an incoming download request comes in from a client, it contains an `aggregation-id` (eg. `shop:mg-manager`) and a `limit` (eg. `12`). This effectively instructs the downloader to only allow at most `12` concurrent download requests for resources with their `aggregation-id` set to `merchant-id:5123`. From a user's perspective, we would only initiate 12 concurrent requests for the Merchant with ID 5123. Note that the `aggregation-id` is opaque; the downloader doesn't know about merchants or any other business domain logic.

- `worker pool`: this is what manages a pool of workers. Each `aggregation` has a respective `worker pool`. The pool is responsible for spawning `workers` and tearing them down. Each pool receives `jobs` for a specific `aggregation` (eg. foobar:10) and delegates the jobs to `workers`

- `processor`: the main component that is responsible for consuming jobs from the queue, managing `worker pools` and feeding the jobs to them.

- `callback`: when a `job` is finished and the resource is available, then a `callback` HTTP request will be initiated by the downloader to the client that requested the resource. This way the client gets notified when the resource it needs is ready.

- `notifier`: The component of `downloader` that is responsible for consuming the result of jobs and notifying back the respective users by issuing HTTP requests to their callback urls.
