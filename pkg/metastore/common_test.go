package metastore

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_getBucketPath(t *testing.T) {
	type args struct {
		location string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		{
			name: "shouldWorkWiths3",
			args: args{
				location: "s3://bucket/table",
			},
			want:  "bucket",
			want1: "table",
		},
		{
			name: "shouldWorkWiths3a",
			args: args{
				location: "s3a://bucket/table",
			},
			want:  "bucket",
			want1: "table",
		},
		{
			name: "shouldWorkWithMulti/",
			args: args{
				location: "s3a://bucket-1/prefix/table",
			},
			want:  "bucket-1",
			want1: "prefix/table",
		},
		{
			name: "shouldWorkWithEnding/",
			args: args{
				location: "s3a://bucket-1/prefix/table/",
			},
			want:  "bucket-1",
			want1: "prefix/table/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, table := getBucketPath(tt.args.location)

			require.Equal(t, tt.want, bucket)
			require.Equal(t, tt.want1, table)
		})
	}
}
