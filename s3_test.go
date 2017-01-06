package bulk

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

func TestS3(t *testing.T) {
	var (
		region             = "ap-southeast-1"
		testBucket         = "testforunittest"
		testBucketNotFound = "testforunittestnotfound"
		testObject         = "testObject"
	)

	sess, _ := session.NewSession(
		&aws.Config{
			Region: aws.String(region),
		},
	)
	metaService := NewS3(
		s3.New(
			sess,
		),
	)

	var (
		m   MetaObject
		err error
	)

	{
		// test non-existing bucket
		m, err = metaService.Object(testBucketNotFound, testObject)
		assert.NoError(t, err)

		err = m.BucketExists()
		assert.Error(t, err)
		assert.IsType(t, ErrNotFound, err)
	}

	// test existing bucket
	m, err = metaService.Object(testBucket, testObject)
	assert.NoError(t, err)

	err = m.BucketExists()
	assert.NoError(t, err)

	err = m.NewBucket()
	assert.NoError(t, err)

	err = m.NewObject()
	assert.NoError(t, err)

	tested, err := m.IsReadOnly()
	assert.NoError(t, err)
	assert.True(t, tested)

	tested, err = m.IsWritable()
	assert.NoError(t, err)
	assert.True(t, tested)

	err = m.MarkRead()
	assert.NoError(t, err)

	tested, err = m.IsReadOnly()
	assert.NoError(t, err)
	assert.True(t, tested)

	tested, err = m.IsWritable()
	assert.NoError(t, err)
	assert.False(t, tested)

	err = m.RemoveObject()
	assert.NoError(t, err)

	tested, err = m.IsReadOnly()
	assert.Error(t, err)
	assert.IsType(t, ErrNotFound, err)
	assert.False(t, tested)

	tested, err = m.IsWritable()
	assert.Error(t, err)
	assert.IsType(t, ErrNotFound, err)
	assert.False(t, tested)
}
