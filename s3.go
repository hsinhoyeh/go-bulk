package bulk

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3 struct {
	svc s3Service
}

func NewS3(svc *s3.S3) S3 {
	return S3{
		svc: s3Service{
			svc: svc,
		},
	}
}

func (m S3) Object(bucket, objectname string) (MetaObject, error) {
	return S3Object{
		svc:        m.svc,
		bucket:     bucket,
		objectname: objectname,
	}, nil
}

type S3Object struct {
	svc s3Service

	bucket     string
	objectname string
}

// NewBucket creates the bucket on s3
// and to avoid duplicated creation, we performs test-and-create protocol
func (s S3Object) NewBucket() error {

	err := s.BucketExists()
	if err == nil {
		return nil
	}

	if err != nil || err != ErrNotFound {
		return err
	}

	params := &s3.CreateBucketInput{
		Bucket: aws.String(s.bucket),
		ACL:    aws.String("public-read-write"),
	}

	_, err = s.svc.CreateBucket(params)
	if err != nil {
		return err
	}
	return nil
}

func (s S3Object) BucketExists() error {
	params := &s3.GetBucketLocationInput{
		Bucket: aws.String(s.bucket),
	}
	_, err := s.svc.GetBucketLocation(params)
	if err != nil {
		return err
	}
	return nil
}

func (s S3Object) NewObject() error {
	params := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectname),
		ACL:    aws.String("public-read-write"),
	}
	_, err := s.svc.PutObject(params)
	if err != nil {
		return err
	}
	return nil
}

func (s S3Object) RemoveObject() error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectname),
	}
	_, err := s.svc.DeleteObject(params)
	if err != nil {
		return err
	}
	return nil
}

func (s S3Object) MarkRead() error {
	// NOTE: MarkRead can work only if the owner of object is the same perple with bucket owner
	params := &s3.PutObjectAclInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectname),
		ACL:    aws.String("public-read"),
	}
	_, err := s.svc.PutObjectAcl(params)
	return err
}

func (s S3Object) URI() string {
	return fmt.Sprintf("https://%s.s3.amazonaws.com/%s", s.bucket, s.objectname)
}

var (
	publicReadable = &s3.Grant{
		Grantee: &s3.Grantee{
			Type: aws.String("Group"),
			URI:  aws.String("http://acs.amazonaws.com/groups/global/AllUsers"),
		},
		Permission: aws.String("READ"),
	}
	publicWritable = &s3.Grant{
		Grantee: &s3.Grantee{
			Type: aws.String("Group"),
			URI:  aws.String("http://acs.amazonaws.com/groups/global/AllUsers"),
		},
		Permission: aws.String("WRITE"),
	}
)

func (s S3Object) IsWritable() (bool, error) {
	grants, err := s.getObjectACL()
	if err != nil {
		return false, err
	}
	for _, grant := range grants {
		if reflect.DeepEqual(grant, publicWritable) {
			return true, nil
		}
	}
	return false, nil

}

func (s S3Object) IsReadOnly() (bool, error) {
	grants, err := s.getObjectACL()
	if err != nil {
		return false, err
	}
	for _, grant := range grants {
		if reflect.DeepEqual(grant, publicReadable) {
			return true, nil
		}
	}
	return false, nil
}

func (s S3Object) getObjectACL() ([]*s3.Grant, error) {
	params := &s3.GetObjectAclInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectname),
	}
	resp, err := s.svc.GetObjectAcl(params)
	if err != nil {
		return nil, err
	}
	return resp.Grants, nil
}

// s3Service wraps s3.Service with a in-house error converter which converts generic awserr into customized defined error
type s3Service struct {
	svc *s3.S3
}

func (ss s3Service) CreateBucket(params *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	resp, err := ss.svc.CreateBucket(params)
	return resp, S3Error(err)
}

func (ss s3Service) GetBucketLocation(params *s3.GetBucketLocationInput) (*s3.GetBucketLocationOutput, error) {
	resp, err := ss.svc.GetBucketLocation(params)
	return resp, S3Error(err)
}

func (ss s3Service) PutObject(params *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	resp, err := ss.svc.PutObject(params)
	return resp, S3Error(err)
}

func (ss s3Service) DeleteObject(params *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	resp, err := ss.svc.DeleteObject(params)
	return resp, S3Error(err)
}

func (ss s3Service) GetObjectAcl(params *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	resp, err := ss.svc.GetObjectAcl(params)
	return resp, S3Error(err)
}

func (ss s3Service) PutObjectAcl(params *s3.PutObjectAclInput) (*s3.PutObjectAclOutput, error) {
	resp, err := ss.svc.PutObjectAcl(params)
	return resp, S3Error(err)
}
