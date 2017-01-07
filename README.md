####go-bulk

package `go-bulk` implements bulk api on top of bulk storage service such as AWS's S3 or GCS.

The advantage of bulk api is that client can upload as many as data without server's intervention. Specifically, clients upload media data or raw data to the underlying bulk storage service directly after they requested urls from `go-bulk` api.

The detailed flow is charted below:

```

                 1.
client     ----------->    bulk service
           <-----------        ^    ^
                 2.            |    |
  |                            |    |
  |-----------------------------    |
  |              4.                 |
  |                                 |
  |                                 V
  |----------------------> bulk storage service (S3, GCS)
                 3.


1. client send an upload request to bulk service
2. bulk service response upload url to client
3. client upload data direcrly to bulk storage service
4. client complete the upload

```

For more details, please refer to unittest.