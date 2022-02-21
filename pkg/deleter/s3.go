package deleter

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type FileDeleterS3 struct {
	s3Client *s3.Client
}

func NewFileDeleterS3(ctx context.Context, region string) (*FileDeleterS3, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &FileDeleterS3{
		s3Client: s3.NewFromConfig(cfg),
	}, nil
}

func (f *FileDeleterS3) Delete(ctx context.Context, bucket string, path string) error {
	objs, err := f.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &path,
	})
	if err != nil {
		return err
	}
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
	return err
}
