![downloader](doc/downloader-service.png)

Async Rate-limited downloader service.

Visit the [wiki](../../wiki/) for documentation.

API
-------------------------------------------------------------------------------

Enqueueing a new download job:
```shell
$ curl -d '{"aggr_id":"aggrFooBar", "aggr_limit":8, "url":"https://httpbin.org/image/png", "callback_url":"http://localhost:8080", "extra":"foobar"}' http://localhost:8000/download
# => {"id":"NSb4FOAs9fVaQw"}
```

Callback requests:
```shell
$ nc -l -p 8080

POST / HTTP/1.1
Host: localhost:8080
User-Agent: Go-http-client/1.1
Content-Length: 93
Content-Type: application/json
Accept-Encoding: gzip

{  
   "success":true,
   "error":"",
   "extra":"foobar",
   "resource_url":"https://httpbin.org/image/png",
   "download_url":"http://localhost/foo/6QE/6QEywYsd0jrKAg",
   "job_id":"6QEywYsd0jrKAg",
   "response_code":200
}

Unsuccessful Callback Examples:

* Resourcce not found

{
   "success":false,
   "error":"Received Status code 404",
   "extra":"foobar",
   "resource_url":"https://httpbin.org/image/png",
   "download_url":"http://localhost/foo/6QE/6QEywYsd0jrKAg",
   "job_id":"6QEywYsd0jrKAg",
   "response_code":404
}

* Invalid TLS Certificate

{
   "success":false,
   "error":"TLS Error Occured: dial: x509: certificate signed by unknown authority ",
   "extra":"foobar",
   "resource_url":"https://httpbin.org/image/png",
   "download_url":"http://localhost/foo/6QE/6QEywYsd0jrKAg",
   "job_id":"6QEywYsd0jrKAg",
   "response_code":0
}

```

Any 2XX response to the callback POST marks the callback as successful for the current job.
As a special case, if a 202 response code is received the job is additionally marked for deletion as not needed any more by the client.
This deletes the job in Redis along with its associated downloaded file.

Development
-------------------------------------------------------------------------------

Run tests, various checks and build project and its dependencies:
```shell
$ make
```

Running the tests:
```shell
$ make test
```
