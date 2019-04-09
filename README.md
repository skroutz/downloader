![downloader](doc/downloader-service.png)

Downloader is a service providing asynchronous and rate-limited download
capabilities. It is entirely written in [Go](https://golang.org/) and backed by
[Redis](https://redis.io/) as a metadata storage backend.

Visit the [wiki](../../wiki/) for documentation.

[![Build Status](https://travis-ci.com/skroutz/downloader.svg?branch=master)](https://travis-ci.com/skroutz/downloader)

Getting Started
-------------------------------------------------------------------------------

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

API
-------------------------------------------------------------------------------

## Endpoints
#### POST /download

Enqueue a new download.
Expects JSON encoded params.
Parameters:

 * `aggr_id`: string, Grouping identifier for the download job.
 * `aggr_limit`: int, Max concurrency limit for the specified group ( aggr_id ).
 * `aggr_proxy`: ( optional ) string, HTTP proxy configuration. It is set up on aggregation level and it cannot be updated for an existing aggregation.
 * `url`: string, The URL pointing to the resource that will get downloaded.
 * `callback_url`: string, The endpoint on which the job callback request will be performed.
 * `extra`: ( optional ) string, Client provided metadata that get passed back in the callback.
 * `mime_type`: ( optional ) string, series of mime types that the download is going to be verified against.

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
A sample configuration can be found in `config.json` file.
It is important to note that the Notifier component depends on the config file
in order to correctly enable the backends that are defined in the config's `backends` key.

If no backends are given the Notifier will throw an error and exit with a non-zero code.
If you want to enable the http backend add the `http` key along with its `timeout` value.
If you want to enable the kafka backend add the `kafka` key along with your desired configuration.

Below you can find examples of jobs enqueueing and callbacks payloads

#### Example using `http` as backend

```shell
$ curl -d '{"aggr_id":"aggrFooBar", "aggr_limit":8, "url":"https://httpbin.org/image/png", "callback_type": "http", "callback_dst":"https://callback.example.com", "extra":"foobar", "mime_type": "!image/vnd.adobe.photoshop,image/*"}' https://downloader.example.com/download
# => {"id":"NSb4FOAs9fVaQw"}
```

#### Example using `kafka` as backend
Suppose you have already configured a kafka cluster and created a topic `dwl_images`.
```shell
$ curl -d '{"aggr_id":"aggrFooBar", "aggr_limit":8, "url":"http://httpbin.org/image/png", "callback_type":"kafka" ,"callback_dst":"dwl_images", "extra":"foobar", "mime_type": "!image/vnd.adobe.photoshop,image/*"}' http://localhost:8000/download
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
   "delivery_error":""
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

Web UI
------------------------------------------------------------------------------

An informational Web UI is served on the default route of the API.
Displayed Info:
* Downloader instances currently running.
* Current active aggregations.
* General statistics reported by the downloader.

Development
-------------------------------------------------------------------------------

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

Credits
-------------------------------------------------
downloader is released under the GNU General Public License version 3. See [COPYING](COPYING).
