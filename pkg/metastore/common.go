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

func getMetadataLocation(metastoreCode MetastoreCode, table model.TableInfo) string {
	location := convertS3Format(metastoreCode, table.MetadataLocation)
	switch table.Format {
	case model.PARQUET:
		return location
	case model.ICEBERG:
		if strings.Contains(location, "/metadata/") {
			return location[0:strings.LastIndex(location, "/metadata/")]
		}
		return location
	default:
		return ""
	}
}

func convertS3Format(metastoreCode MetastoreCode, location string) string {
	switch metastoreCode {
	case GLUE:
		location = strings.ReplaceAll(location, "s3a://", "s3://")
	case HIVE:
		location = strings.ReplaceAll(location, "s3://", "s3a://")
	}
	return location
}

func stringFromPtr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
