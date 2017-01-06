package bulk

import (
	"errors"
	"log"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

var (
	ErrNotFound  = errors.New("bulk: not found")
	ErrUnknown   = errors.New("bulk: error unknown")
	ErrImmutable = errors.New("bulk: object is immutable now")
)

func S3Error(err error) error {
	if err == nil {
		return nil
	}
	aerr, ok := err.(awserr.Error)
	if !ok {
		return err
	}
	switch aerr.Code() {
	case "NoSuchBucket":
		return ErrNotFound
	case "NoSuchKey":
		return ErrNotFound
	default:
		log.Printf("unknown:%v\n", err)
		return ErrUnknown
	}
	return nil
}
