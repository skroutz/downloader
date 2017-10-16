# Job/aggregation flow

This document details the complete lifecycle of a job, from the time it is
received until its callback is executed.


1. `API server` receives a new job request
    - if the aggregation does not exist, create it (`HSET aggr:msystems`)
    - persist job (`HMSET job:1323 id 1323 aggr_id msystems url http://foo.bar/a.jpg`)
    - enqueue job (`RPUSH jobs:msystems 1323`)
2. `Processor` periodically scans for new aggregations (`SCAN aggr:*`)
    - if a new aggregation is found, spawn a `WorkerPool` for it
3.  The `WorkerPool` for the _"msystems"_ aggregation pops jobs enqueued in
    step (1) (`LPOP jobs:msystems`)
      1. if new jobs are found and the pool's workers count limit is not reached,
        spawn a new worker
      2. feed the job to one of the workers (`workerPool.jobChan <- job`)
4. Each worker of the `WorkerPool` receives jobs and for each job it:
   1. performs the actual job (ie. downloads the file) and updates the job's
      download state depending on the outcome (`HMSET job:1323 download_state SUCCESS`) and
   2. enqueues the job for callback (`RPUSH callbackQueue 1323`) and initializes
      its callback state to `PENDING`
5. `Notifier` consumes from the callback queue, performs the corresponding
   callback requests (`LPOP callbackQueue`) and updates the job callback state
   accordingly


