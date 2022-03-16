package model

import (
	"fmt"
	"strconv"
	"strings"
)

type SerDeInfo struct {
	SerializationLib string
	Parameters       map[string]string
}

type TableFormat string

const (
	PARQUET        TableFormat = "parquet"
	ICEBERG                    = "iceberg"
	EXTERNAL_TABLE             = "EXTERNAL_TABLE"
)

func FromInputOutput(input string) TableFormat {
	switch input {
	case "org.apache.hadoop.hiveFactory.ql.io.parquet.MapredParquetInputFormat":
		fallthrough
	case "org.apache.hadoop.hiveFactory.ql.io.parquet.MapredParquetOutputFormat":
		fallthrough
	case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat":
		fallthrough
	case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat":
		return PARQUET
	default:
		return ICEBERG
	}
}

func (t TableFormat) InputFormat() string {
	switch t {
	case PARQUET:
		return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
	case ICEBERG:
		return ""
	default:
		return ""
	}
}

func (t TableFormat) OutputFormat() string {
	switch t {
	case PARQUET:
		return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
	case ICEBERG:
		return ""
	default:
		return ""
	}
}

func (t TableFormat) SerDeInfo() *SerDeInfo {
	switch t {
	case PARQUET:
		return &SerDeInfo{
			Parameters:       t.SerdeParameters(),
			SerializationLib: t.SerdeLibrary(),
		}
	case ICEBERG:
		return nil
	default:
		return nil
	}
}

func (t TableFormat) SerdeLibrary() string {
	switch t {
	case PARQUET:
		return "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
	case ICEBERG:
		return ""
	default:
		return ""
	}
}

func (t TableFormat) SerdeParameters() map[string]string {
	switch t {
	case PARQUET:
		return map[string]string{
			"serialization.format": "1",
		}
	case ICEBERG:
		return nil
	default:
		return nil
	}
}

func (t TableFormat) Parameters(location string) map[string]string {
	switch t {
	case PARQUET:
		return map[string]string{
			"EXTERNAL": "TRUE",
		}
	case ICEBERG:
		return map[string]string{
			"metadata_location": location,
			"table_type":        ICEBERG,
		}
	default:
		return nil
	}
}

func (t TableFormat) TableType() string {
	switch t {
	case PARQUET:
		return EXTERNAL_TABLE
	case ICEBERG:
		return ICEBERG
	default:
		return ""
	}
}

type DropArg struct {
	Db     string      `json:"db"`
	Tables []DropTable `json:"tables"`
}

type DropTable struct {
	Table      string `json:"table"`
	DeleteData bool   `json:"delete_data"`
}

type DatabaseTables struct {
	Db     string      `json:"db"`
	Tables []TableInfo `json:"tables"`
}

type TableInfo struct {
	Name             string      `json:"name"`
	Columns          []Column    `json:"columns"`
	Partitions       []Column    `json:"partitions"`
	MetadataLocation string      `json:"metadata_location"`
	Format           TableFormat `json:"format"`
}

type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

func MapColumnType(t string) ColumnType {
	if strings.HasPrefix(t, "varchar") {
		length, err := strconv.Atoi(t[8 : len(t)-1])
		if err != nil {
			length = 1024
		}
		return ColumnType{
			SqlType: VARCHAR,
			Length:  length,
		}
	}
	return ColumnType{
		SqlType: SqlType(t),
	}
}

func UnmapColumnType(t ColumnType) string {
	if t.SqlType == VARCHAR {
		return fmt.Sprintf("%s(%d)", VARCHAR, t.Length)
	}
	return string(t.SqlType)
}

type ColumnType struct {
	SqlType SqlType `json:"sql_type"`
	Length  int     `json:"length"`
}

type SqlType string

const (
	VARCHAR   SqlType = "varchar"
	INTEGER           = "int"
	BIGINT            = "bigint"
	SMALLINT          = "smallint"
	DOUBLE            = "double"
	DATE              = "date"
	TIMESTAMP         = "timestamp"
	BOOLEAN           = "boolean"
)

func strPtr(s string) *string {
	return &s
}
