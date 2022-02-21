package metastore

import (
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
