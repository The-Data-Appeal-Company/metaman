package deleter

import "context"

type FileDeleter interface {
	Delete(ctx context.Context, bucket, path string) error
}
