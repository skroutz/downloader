# Redis schema & access patterns

## Schema

We have the following types of keys in Redis:

1. aggregation keys: Each aggregation has a corresponding Redis Hash named in
   the form of `<AggrKeyPrefix><aggregation-id>`. Example: `aggr:beststore`

2. jobs list keys: The Job IDs of each individual aggregation exist in a Redis
   List named in the form of `<JobsKeyPrefix><aggregation-id>`. Example:
   `jobs:beststore`

3. job keys: Each Job has a corresponding Redis Hash named in the form
   `<JobKeyPrefix><job-id>`. Example: `job:3241`

5. callback queue: A Redis List that contains the job IDs of either failed or
   completed jobs.


## Access patterns

Here we document which component performs which type of operation (read, write)
on each key. It should reflect the `master` branch.

|               | aggregation | job lists | jobs | callback queue |
| ------------- |:----------:| :------:| :-----: | :-----: |
| API Server    | RW | RW | RW | - |
| Processor     | R | RW | RW | W |
| Notifier      | - | - | RW | RW |



