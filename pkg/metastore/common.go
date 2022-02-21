package metastore

import (
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
	"strings"
)

func getBucketPath(location string) (string, string) {
	prefix := ""
	if strings.HasPrefix(location, "s3://") {
		prefix = "s3://"
	} else if strings.HasPrefix(location, "s3a://") {
		prefix = "s3a://"
	}
	locationNoProtocol := location[strings.Index(location, prefix)+len(prefix):]
	bucket := locationNoProtocol[:strings.Index(locationNoProtocol, "/")]
	path := locationNoProtocol[strings.Index(locationNoProtocol, "/")+1:]
	return bucket, path
}

func isOnS3(location string) bool {
	return strings.HasPrefix(location, "s3://") || strings.HasPrefix(location, "s3a://")
}

func getMetadataLocation(table model.TableInfo) string {
	switch table.Format {
	case model.PARQUET:
		return table.MetadataLocation
	case model.ICEBERG:
		return table.MetadataLocation[0:strings.LastIndex(table.MetadataLocation, "/metadata/")]
	default:
		return ""
	}
}
