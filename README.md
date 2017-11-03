![downloader](doc/downloader-service.png)

Async Rate-limited downloader service.

Visit the [wiki](wiki/) for documentation.


API
-------------------------------------------------------------------------------

Enqueueing a new download job:
```shell
$ curl -d '{"aggr_id":"aggrFooBar", "aggr_limit":8, "url":"https://httpbin.org/image/png", "callback_url":"http://localhost:8080", "extra":"foobar"}' http://localhost:8000/download
{"id":"NSb4FOAs9fVaQw"}
```

Callback requests:
```shell
$ nc -l 8080

POST / HTTP/1.1
Host: localhost:8080
User-Agent: Go-http-client/1.1
Content-Length: 93
Content-Type: application/json
Accept-Encoding: gzip

{"success":true,"error":"","extra":"foobar","download_url":"http://localhost/NSb4FOAs9fVaQw"}
```



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

For more see:
```shell
$ make list
```



