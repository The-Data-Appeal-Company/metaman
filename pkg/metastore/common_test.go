package metastore

import (
	"github.com/stretchr/testify/require"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
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

func Test_getMetadataLocation(t *testing.T) {
	type args struct {
		table model.TableInfo
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "shouldGetMetadataLocationIceberg",
			args: args{
				table: model.TableInfo{
					MetadataLocation: "s3://bucket/tests/schema/table/metadata/00000-8051da97-485b-4715-b22f-6302b46c752e.metadata.json",
					Format:           model.ICEBERG,
				},
			},
			want: "s3://bucket/tests/schema/table",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, getMetadataLocation(tt.args.table))
		})
	}
}
