package metastore

import (
	"context"
	"fmt"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/sirupsen/logrus"
	"github.com/the-Data-Appeal-Company/metaman/pkg/deleter"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
)

type Hive interface {
	GetTable(dbName string, tableName string) (*hive_metastore.Table, error)
	GetAllTables(dbName string) ([]string, error)
	CreateTable(table *hive_metastore.Table) error
	DropTable(dbName string, tableName string, deleteData bool) error
}

type HiveMetaStore struct {
	hive        Hive
	fileDeleter deleter.FileDeleter
}

func NewHiveMetaStore(hive Hive, fileDeleter deleter.FileDeleter) *HiveMetaStore {
	return &HiveMetaStore{hive: hive, fileDeleter: fileDeleter}
}

func (h *HiveMetaStore) GetTables(dbName string) ([]string, error) {
	return h.hive.GetAllTables(dbName)
}

func (h *HiveMetaStore) GetTableInfo(dbName, tableName string) (model.TableInfo, error) {
	table, err := h.hive.GetTable(dbName, tableName)
	if err != nil {
		return model.TableInfo{}, err
	}
	return model.TableInfo{
		Name:             table.GetTableName(),
		Columns:          mapColumnsHive(table.Sd.Cols),
		MetadataLocation: table.Sd.Location,
		Format:           model.FromInputOutput(table.Sd.InputFormat),
	}, nil
}

func (h *HiveMetaStore) CreateTable(dbName string, table model.TableInfo) error {
	if len(table.Columns) == 0 {
		return fmt.Errorf("cannot Create table with 0 columns")
	}
	return h.hive.CreateTable(&hive_metastore.Table{
		TableName: table.Name,
		DbName:    dbName,
		Sd: &hive_metastore.StorageDescriptor{
			Cols:         unmapColumnsHive(table.Columns),
			Location:     table.MetadataLocation,
			InputFormat:  table.Format.InputFormat(),
			OutputFormat: table.Format.OutputFormat(),
			SerdeInfo:    mapSerdeInfoHive(table.Format.SerDeInfo()),
		},
		Parameters: table.Format.Parameters(table.MetadataLocation),
	})
}

func (h *HiveMetaStore) DropTable(dbName string, tableName string, deleteData bool) error {
	info, err := h.GetTableInfo(dbName, tableName)
	if err != nil {
		return err
	}
	err = h.hive.DropTable(dbName, tableName, deleteData)
	if err != nil {
		return err
	}
	if deleteData {
		if isOnS3(info.MetadataLocation) {
			bucket, path := getBucketPath(info.MetadataLocation)
			err := h.fileDeleter.Delete(context.Background(), bucket, path)
			if err != nil {
				logrus.Errorf("table dropped on hive but could not delete files if they are on s3")
				return err
			}
		}
	}
	return nil
}

func unmapColumnsHive(columns []model.Column) []*hive_metastore.FieldSchema {
	cols := make([]*hive_metastore.FieldSchema, len(columns))
	for i, column := range columns {
		cols[i] = &hive_metastore.FieldSchema{
			Name: column.Name,
			Type: model.UnmapColumnType(column.Type),
		}
	}
	return cols
}

func mapColumnsHive(cols []*hive_metastore.FieldSchema) []model.Column {
	columns := make([]model.Column, len(cols))
	for i, col := range cols {
		columns[i] = model.Column{
			Name: col.Name,
			Type: model.MapColumnType(col.Type),
		}
	}
	return columns
}

func mapSerdeInfoHive(info *model.SerDeInfo) *hive_metastore.SerDeInfo {
	if info == nil {
		return nil
	}
	return &hive_metastore.SerDeInfo{
		SerializationLib: info.SerializationLib,
		Parameters:       info.Parameters,
	}
}
