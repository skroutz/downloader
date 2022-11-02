![downloader](doc/downloader-service.png)

Downloader is a service providing asynchronous and rate-limited download
capabilities. It is entirely written in [Go](https://golang.org/) and backed by
[Redis](https://redis.io/) as a metadata storage backend.

Visit the [wiki](../../wiki/) for documentation.

[![CI](https://github.com/skroutz/downloader/actions/workflows/ci.yml/badge.svg)](https://github.com/skroutz/downloader/actions/workflows/ci.yml)

# Getting Started

Make sure that you have a working [Go
environment](https://golang.org/doc/install) and that you have configured your
[`$GOPATH`](https://golang.org/doc/code.html#Workspaces) correctly.

Clone the repository or `go get` it with:
```shell
$ go get github.com/skroutz/downloader
```

Dependencies are managed using [dep](https://github.com/golang/dep). So after
cloning the project, just run `make` to install the dependencies and build the
package from the project directory `$GOPATH/src/github.com/skroutz/downloader/`:
```shell
$ make
```

Finally, to install the Downloader, just run:
```shell
$ make install
```

For more information about the supported make targets, please read the
**Synopsis** sections in the package's Makefile.

Enjoy! :)

# API

## Endpoints
#### POST /download

Enqueue a new download.
Expects JSON encoded params.
Parameters:

 * `aggr_id`: string, Grouping identifier for the download job.
 * `aggr_limit`: int, Max concurrency limit for the specified group ( aggr_id ).
 * `aggr_proxy`: ( optional ) string, HTTP proxy configuration. It is set up on aggregation level and it cannot be updated for an existing aggregation.
 * `url`: string, The URL pointing to the resource that will get downloaded.
 * `callback_type`: ( optional if `s3_bucket` is set) string, The callback backend type. Either `http` or `kafka` or `sqs`. Deprecates `callback_url`.
 * `callback_url`: ( optional if `s3_bucket` is set) string, The endpoint on which the job callback request will be performed.
 * `callback_dst`: ( optional if `s3_bucket` is set) string, The endpoint on which the job callback request will be performed. Deprecates `callback_url`.
 * `extra`: ( optional ) string, Client provided metadata that get passed back in the callback.
 * `mime_type`: ( optional ) string, series of mime types that the download is going to be verified against.
 * `max_retries`: ( optional ) int, Maximum download retries when retryiable errors are encountered.
 * `extract_image_size`: ( optional ) boolean, Compute image size on supported mime-types (jpeg, png, gif). For unsupported mime-types this is ignored.
 * `download_timeout`: ( optional ) int, HTTP client timeout per Job, in seconds.
 * `request_headers`: ( optional ) object{string => string}, HTTP Request Headers per job.
 * `s3_bucket`: ( optional ) string, requires `s3_region`, the caller-owned AWS S3 bucket to store the downloaded object. IAM access should be setup beforehand.
 * `s3_region`: ( optional ) string, requires `s3_bucket`, the AWS region of the caller-owned AWS S3 bucket.

Output: JSON document containing the download's id e.g, `{"id":"NSb4FOAs9fVaQw"}`

#### GET /hb
Acts as a heartbeat for the downloader instance.
Depending on the existence of a certain file on disk returns HTTP status code 503 if path exists, 200 otherwise.

#### POST /retry/:job_id
Retries the callback of the job with the specified id.
Returns HTTP status 201 on success.

#### GET /dashboard/aggregations
Returns a JSON list of aggregations with pending jobs.

Output: JSON array of aggregation names and their pending jobs `[{"name":"jobs:super-aggregation","size":17}]`

## Usage

### Configuration
Downloader requires a `config.json` present.
A sample configuration can be found in `config.json.sample` file.
It is important to note that the Notifier component depends on the config file
in order to correctly enable the backends that are defined in the config's `backends` key.

If no backends are given the Notifier will throw an error and exit with a non-zero code.
If you want to enable the http backend add the `http` key along with its `timeout` value.
If you want to enable the kafka backend add the `kafka` key along with your desired configuration.
If you want to enable the sqs backend add the `sqs` key along with its `region` value.

#### Storage backend
Downloader is able to store files on an AWS S3 bucket instead of a filesystem. This is possible by providing a
`filestorage` section in the configuration file, in which case the `storage_dir` path becomes
the temporary filesystem storage.

Example using an AWS S3 bucket as the storage backend:
```
processor": {
    "filestorage": {
        "type": "s3",
        "bucket": "mybucketname",
        "region": "eu-west-2"
    },
    "storage_dir": "/tmp",
```

Example using a filesystem as the storage backend:
```
processor": {
    "filestorage": {
        "type": "filesystem",
        "rootdir": "/var/lib/downloader",
    },
    "storage_dir": "/tmp",
```

Using just a `storage_dir` without a `filestorage` section is still possible but considered deprecated.

Note: When a download request provides its own S3 bucket/region, the configured filestorage is ignored for this job.

Below you can find examples of jobs enqueueing and callbacks payloads

#### Example using `http` as backend

```shell
$ curl -XPOST -d '{"aggr_id":"aggrFooBar", "aggr_limit":8, "url":"https://httpbin.org/image/png", "callback_type": "http", "callback_dst":"https://callback.example.com", "extra":"foobar", "mime_type": "!image/vnd.adobe.photoshop,image/*", "request_headers": {"Accept":"image/png,image/jpeg,image/*,*/*","User-Agent":"Downloader-Agent"}}' https://downloader.example.com/download
# => {"id":"NSb4FOAs9fVaQw"}
```

#### Example using `kafka` as backend
Suppose you have already configured a kafka cluster and created a topic `dwl_images`.
```shell
$ curl -XPOST -d '{"aggr_id":"aggrFooBar", "aggr_limit":8, "url":"http://httpbin.org/image/png", "callback_type":"kafka" ,"callback_dst":"dwl_images", "extra":"foobar", "mime_type": "!image/vnd.adobe.photoshop,image/*", "request_headers": {"Accept":"image/png,image/jpeg,image/*,*/*","User-Agent":"Downloader-Agent"}}' http://downloader.example.com/download
# => {"id":"Hl2VErjyL5UK9A"}
```

#### Example using `sqs` as backend
Suppose you have already configured and created an sqs queue `downloader_notifications` in the region that is in the configuration file.
```shell
$ curl -XPOST -d '{"aggr_id":"aggrFooBar", "aggr_limit":8, "url":"http://httpbin.org/image/png", "callback_type":"sqs" ,"callback_dst":"aws.com/sqs/downloader_notifications", "extra":"foobar", "mime_type": "!image/vnd.adobe.photoshop,image/*", "request_headers": {"Accept":"image/png,image/jpeg,image/*,*/*","User-Agent":"Downloader-Agent"}}' http://downloader.example.com/download
# => {"id":"Hl2VErjyL5UK9A"}
```

Example Callback payloads:

 * Successful download callback:

```json
{
   "success":true,
   "error":"",
   "extra":"foobar",
   "resource_url":"https://httpbin.org/image/png",
   "download_url":"http://localhost/foo/6QE/6QEywYsd0jrKAg",
   "job_id":"6QEywYsd0jrKAg",
   "response_code":200,
   "delivered":true,
   "delivery_error":"",
   "image_size": "10x10"
}
```

Unsuccessful Callback Examples:

 * Resourcce not found

```json
{
   "success":false,
   "error":"Received Status code 404",
   "extra":"foobar",
   "resource_url":"https://httpbin.org/image/png",
   "download_url":"http://localhost/foo/6QE/6QEywYsd0jrKAg",
   "job_id":"6QEywYsd0jrKAg",
   "response_code":404,
   "delivered":true,
   "delivery_error":""
}
```

* Invalid TLS Certificate

```json
{
   "success":false,
   "error":"TLS Error Occured: dial: x509: certificate signed by unknown authority",
   "extra":"foobar",
   "resource_url":"https://httpbin.org/image/png",
   "download_url":"http://localhost/foo/6QE/6QEywYsd0jrKAg",
   "job_id":"6QEywYsd0jrKAg",
   "response_code":0,
   "delivered":true,
   "delivery_error":""
}
```

* Mime Type mismatch

```json
{
   "success":false,
   "error":"Expected mime-type to be (image/jpeg), found (image/png)",
   "extra":"foobar",
   "resource_url":"https://httpbin.org/image/png",
   "download_url":"http://localhost/foo/6QE/6QEywYsd0jrKAg",
   "job_id":"6QEywYsd0jrKAg",
   "response_code":200,
   "delivered":true,
   "delivery_error":""
}
```

For http as a notifier backend any 2XX response to the callback POST marks the callback as successful for the current job.
For kafka as a notifier backend, we monitor kafka's `Events` channel and mark a job's callback as successful if the delivery report
of a job's callback has been received and has no errors.

Setting a callback becomes optional if the caller has provided an AWS S3 bucket to store the downloaded file. This is because
it is possible to use AWS S3 object operations as event triggers directly.

# Web UI

An informational Web UI is served on the default route of the API.
Displayed Info:
* Downloader instances currently running.
* Current active aggregations.
* General statistics reported by the downloader.

# Development

> :warning: You should have a *running* Redis instance in order to be able to
> run the downloader's tests. Make sure to update the corresponding setting in
> the [configuration file](config.test.json).

In case you haven't done it already (as described in the [Getting
Started](#getting-started) section), run:
```shell
$ make
```
to manage the project's dependencies. Now, you will have a fully functioning
development environment.

To run the tests and perform various package-related checks, just run:
```shell
$ make check
```

## Redis

For convenience, we provide a docker-compose.yml which starts a redis instance,
which you can use for the checks or manual testing.

To start redis:

```shell
$ docker-compose up redis
```

or to start in the background:

```shell
$ docker-compose start
$ docker-compose stop
```

The above requires you to have `docker-compose` installed. If you have not,
see [here](https://docs.docker.com/compose/install/).

## Docker image

Currently, we maintain a containers' "anti-pattern" for running downloader, that is having a single
container image for running all downloader processes, i.e., API, Processor, and Notifier. To do so,
we have a very simple wrapper entrypoint script, which spawns all three processes one by one and
then just waits for them until exit.

In order to build and run the container, given that you have a Redis instance running you only have
to perform the following actions:

```shell
$ docker build -t downloader:devel . -f Dockerfile
$ docker run --rm --name downloader --network host downloader:devel
```

However, if you want to only run a single service (let's say the API), or use a custom config file,
we may run a command like the following:

```shell
$ docker run --rm \
  -v /your-path/a-config-file.json:/custom-config.json \
  --name downloader --network host --entrypoint /srv/downloader \
  downloader:devel api -c /custom-config.json --port 8000 --host 0.0.0.0
```

# Deploying to AWS

On AWS, we maintain separate accounts per environment, i.e., *staging*, *prod*, or *test* in the
future, where we have deployed the same infrastructure but with the resources' count adopted based
on the account's usage, apparently. The accounts' infrastructure setup comes from a separate
repository that is the [aws-downloader](https://github.com/skroutz/aws-downloader), and each
environment is denoted by the respective branch, i.e, *staging*, *prod*, etc.

In order to deploy the downloader service on AWS we follow a similar appoarch to the Infrastructure
deployments. So consider that you are working on a new feature for downloader under a branch named
*feature-x* and the testing of the feature is completed (let's say locally via Docker), the next
step would be to test the feature on staging account. This is as easy as pushing your feature on
staging remote branch and the respective GitHub Action will be triggered to deploy the application
on AWS. The last step, would be to open a PR against the master branch; as soon as the PR get's
approved and merged to master, the production GHA will be auto-triggered to deploy the application
to the production AWS account.

> TODO: We still need to consider the [deployment
> strategy](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-types.html) we
> intend to follow.

## AWS GitHub Action Workflow

The current GitHub Action (`aws_ecs.yml`) is a [Reusable
worklow](https://docs.github.com/en/actions/using-workflows/reusing-workflows) which will be used
to deploy downloader to the respective environment with the specified arguments and options. The
workflow is based on the official [GitHub
guide](https://docs.github.com/en/actions/deployment/deploying-to-your-cloud-provider/deploying-to-amazon-elastic-container-service)
for deploying resources to ECS and it is used to:

- Build the downloader container image
- Push the image to Amazon Elastic Container Registry (ECR)
- Deploy it to Amazon Elastic Container Service (ECS)

In order to access resources on AWS, we follow GitHub's best practices and make use of [OpenID
Connect](https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/about-security-hardening-with-openid-connect).
Authorization to AWS is a two step process; we first need to assume the GitHub Action role of the
central CI account using OIDC (`AWS_GHA_ROLE_TO_ASSUME` secret) and then assume the account's
application deployment role (`AWS_GHA_EXEC_ROLE_TO_ASSUME` secret) prior to execute any actions on
AWS. The secret on GitHub are stored using *GitHub environments* with branch protection rules
applied.

# Credits

downloader is released under the GNU General Public License version 3. See [COPYING](COPYING).
