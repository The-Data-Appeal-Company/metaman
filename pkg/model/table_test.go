package model

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTableFormat_InputFormat(t *testing.T) {
	tests := []struct {
		name string
		t    TableFormat
		want string
	}{
		{
			name: "shouldParquet",
			t:    PARQUET,
			want: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.t.InputFormat())
		})
	}
}

func TestTableFormat_OutputFormat(t *testing.T) {
	tests := []struct {
		name string
		t    TableFormat
		want string
	}{
		{
			name: "shouldParquet",
			t:    PARQUET,
			want: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.t.OutputFormat())
		})
	}
}

func TestFromInputOutput(t *testing.T) {
	type args struct {
		input string
	}
	tests := []struct {
		name string
		args args
		want TableFormat
	}{
		{
			name: "shouldOutputParquet",
			args: args{
				input: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
			},
			want: PARQUET,
		},
		{
			name: "shouldInputParquet",
			args: args{
				input: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			},
			want: PARQUET,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromInputOutput(tt.args.input); got != tt.want {
				t.Errorf("FromInputOutput() = %v, want %v", got, tt.want)
			}
		})
	}
}
