package bulk

import (
	"fmt"
	"hash/fnv"
)

// Uploader contains operators for managing uploading process
type Uploader interface {
	// Request requests a upload and a download urls associated to the $key
	// requests with same $key will get the same response
	Request(key string) (*UploadResponse, error)

	// Complete marks the uploading to the $key is finished, no further uploading is required.
	Complete(key string) error

	// Delete removed the object
	Delete(key string) error
}

// UploadResponse defines the response for the request call
type UploadResponse struct {
	URL string `json:"url"`
}

type MetaObject interface {
	//  NewBucket creates bucket to its backend
	NewBucket() error
	// BucketExists tests the existance of the bucket
	BucketExists() error
	// NewObject creates an object to its backend
	NewObject() error
	// RemoveObject removes the objects
	RemoveObject() error
	// MarkRead marks immutable property on the object
	MarkRead() error
	// URI returns uri to access this object
	URI() string
	// IsWriteable returns the tested result of writable to that object
	IsWritable() (bool, error)
	// IsReadOnly returns the tested result of read only to that object
	IsReadOnly() (bool, error)
}

type MetaService interface {
	Object(bucket, objectname string) (MetaObject, error)
}

type defaultUploader struct {
	service MetaService
	bucket  string
}

func NewUploader(service MetaService, bucket string) Uploader {
	return defaultUploader{
		service: service,
		bucket:  bucket,
	}
}

// Request requests a upload and a download urls associated to the $key
// requests with same $key will get the same response
func (d defaultUploader) Request(key string) (*UploadResponse, error) {
	metaObject, err := d.service.Object(d.bucket, normalizedKey(key))
	if err != nil {
		return nil, err
	}
	r, err := metaObject.IsReadOnly()
	if err == nil || r {
		return nil, ErrImmutable
	}
	if err != nil && err != ErrNotFound {
		return nil, err
	}

	err = metaObject.NewObject()
	if err != nil {
		return nil, err
	}
	return &UploadResponse{
		URL: metaObject.URI(),
	}, nil
}

// Complete marks the uploading to the $key is finished, no further uploading is required.
func (d defaultUploader) Complete(key string) error {
	metaObject, err := d.service.Object(d.bucket, normalizedKey(key))
	if err != nil {
		return err
	}
	return metaObject.MarkRead()
}

// Delete removed the object
func (d defaultUploader) Delete(key string) error {
	metaObject, err := d.service.Object(d.bucket, normalizedKey(key))
	if err != nil {
		return err
	}
	return metaObject.RemoveObject()
}

func normalizedKey(key string) string {
	fnv1a := fnv.New64a()
	fnv1a.Write([]byte(key))
	return fmt.Sprintf("%d", fnv1a.Sum64())
}
