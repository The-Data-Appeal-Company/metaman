package deleter

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Client interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type FileDeleterS3 struct {
	s3Client S3Client
}

func NewFileDeleterS3(s3client S3Client) *FileDeleterS3 {
	return &FileDeleterS3{
		s3Client: s3client,
	}
}

func (f *FileDeleterS3) Delete(ctx context.Context, bucket string, path string) error {
	isTruncated := true
	var token *string
	for isTruncated {
		objs, err := f.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			ContinuationToken: token,
			Prefix:            &path,
		})
		if err != nil {
			return err
		}
		token = objs.ContinuationToken
		isTruncated = objs.IsTruncated

		ids := make([]types.ObjectIdentifier, len(objs.Contents))
		for i := range objs.Contents {
			ids[i] = types.ObjectIdentifier{Key: objs.Contents[i].Key}
		}
		_, err = f.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &bucket,
			Delete: &types.Delete{
				Objects: ids,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
