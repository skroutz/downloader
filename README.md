![downloader](doc/downloader-service.png)

Downloader is a service providing asynchronous and rate-limited download
capabilities. It is entirely written in [Go](https://golang.org/) and backed by
[Redis](https://redis.io/) as a metadata storage backend.

Visit the [wiki](../../wiki/) for documentation.

Getting Started
-------------------------------------------------------------------------------

Make sure that you have a working [Go
environment](https://golang.org/doc/install) and that you have configured your
[`$GOPATH`](https://golang.org/doc/code.html#Workspaces) correctly.

Clone the repository or `go get` it with:
```shell
$ go get github.com/skroutz/downloader
```

Dependencies are managed using [dep](https://github.com/golang/dep), so after
cloning the project, just run:
```shell
$ dep ensure
```
from the project directory: `$GOPATH/src/github.com/skroutz/downloader/`.

Finally, you build and install the Downloader:
```shell
$ go build
$ go install
```

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
 * `url`: string, The URL pointing to the resource that will get downloaded.
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

Enqueueing a new download job:
```shell
$ curl -d '{"aggr_id":"aggrFooBar", "aggr_limit":8, "url":"https://httpbin.org/image/png", "callback_url":"https://callback.example.com", "extra":"foobar", "mime_type": "!image/vnd.adobe.photoshop,image/*"}' https://downloader.example.com/download
# => {"id":"NSb4FOAs9fVaQw"}
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
   "response_code":200
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
   "response_code":404
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
   "response_code":0
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
   "response_code":200
}
```

Any 2XX response to the callback POST marks the callback as successful for the current job.
As a special case, if a 202 response code is received the job is additionally marked for deletion as not needed any more by the client.
This deletes the job in Redis along with its associated downloaded file.

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
$ dep ensure
```
to manage the project's dependencies. Now, you will have a fully functioning
development environment.

Use `make` to run tests, perform various checks and build project and its
dependencies:
```shell
$ make
```

E.g. to run the tests:
```shell
$ make test
```

Credits
-------------------------------------------------
downloader is released under the GNU General Public License version 3. See [COPYING](COPYING).
