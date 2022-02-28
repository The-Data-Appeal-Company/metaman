package deleter

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/require"
	"testing"
)

type DeleteCall struct {
	bucket string
	keys   []string
}

type S3Mock struct {
	listError   error
	deleteError error
	deleteCalls []DeleteCall
}

func (s *S3Mock) ListObjectsV2(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if s.listError != nil {
		return nil, s.listError
	}
	if params.ContinuationToken == nil {
		return &s3.ListObjectsV2Output{
			Contents: []types.Object{
				{
					Key: aws.String("file_1"),
				},
			},
			ContinuationToken: aws.String("tok"),
			IsTruncated:       true,
		}, nil
	} else if *params.ContinuationToken == "tok" {
		return &s3.ListObjectsV2Output{
			Contents: []types.Object{
				{
					Key: aws.String("file_2"),
				},
			},
			IsTruncated: false,
		}, nil
	}
	return nil, fmt.Errorf("error")
}

func (s *S3Mock) DeleteObjects(_ context.Context, params *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	keys := make([]string, len(params.Delete.Objects))
	for i, object := range params.Delete.Objects {
		keys[i] = *object.Key
	}
	s.deleteCalls = append(s.deleteCalls, DeleteCall{
		bucket: *params.Bucket,
		keys:   keys,
	})
	if s.deleteError != nil {
		return nil, s.deleteError
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func TestFileDeleterS3_Delete(t *testing.T) {
	type fields struct {
		s3Client S3Client
	}
	type args struct {
		ctx    context.Context
		bucket string
		path   string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "shouldDeleteOnS3MultiPage",
			fields: fields{
				s3Client: &S3Mock{},
			},
			args: args{
				ctx:    context.Background(),
				bucket: "bucket",
				path:   "patto",
			},
			wantErr: false,
		},
		{
			name: "shouldErrorOnListError",
			fields: fields{
				s3Client: &S3Mock{
					listError: fmt.Errorf("error"),
				},
			},
			args: args{
				ctx:    context.Background(),
				bucket: "bucket",
				path:   "patto",
			},
			wantErr: true,
		},
		{
			name: "shouldErrorOnDeleteError",
			fields: fields{
				s3Client: &S3Mock{
					deleteError: fmt.Errorf("error"),
				},
			},
			args: args{
				ctx:    context.Background(),
				bucket: "bucket",
				path:   "patto",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFileDeleterS3(tt.fields.s3Client)
			if err := f.Delete(tt.args.ctx, tt.args.bucket, tt.args.path); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			mock := tt.fields.s3Client.(*S3Mock)
			if mock.listError != nil {
				require.Len(t, mock.deleteCalls, 0)
				return
			}
			if mock.deleteError != nil {
				require.Len(t, mock.deleteCalls, 1)
				require.Equal(t, tt.args.bucket, mock.deleteCalls[0].bucket)
				require.Equal(t, []string{"file_1"}, mock.deleteCalls[0].keys)
				return
			}
			require.Len(t, mock.deleteCalls, 2)
			require.Equal(t, tt.args.bucket, mock.deleteCalls[0].bucket)
			require.Equal(t, []string{"file_1"}, mock.deleteCalls[0].keys)
			require.Equal(t, tt.args.bucket, mock.deleteCalls[1].bucket)
			require.Equal(t, []string{"file_2"}, mock.deleteCalls[1].keys)
		})
	}
}
