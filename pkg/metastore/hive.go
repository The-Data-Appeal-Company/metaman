package metastore

import (
	"context"
	"fmt"
	"github.com/akolb1/gometastore/hmsclient"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/sirupsen/logrus"
	"github.com/the-Data-Appeal-Company/metaman/pkg/deleter"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
	"strings"
)

type Hive interface {
	GetTable(dbName string, tableName string) (*hive_metastore.Table, error)
	GetAllTables(dbName string) ([]string, error)
	CreateTable(table *hive_metastore.Table) error
	DropTable(dbName string, tableName string, deleteData bool) error
	Close()
}

type HiveFactory interface {
	GetHive() (Hive, error)
}

type HiveAlwaysRecreateFactory struct {
	hiveHost string
	hivePort int
}

func NewHiveAlwaysRecreateFactory(hiveHost string, hivePort int) *HiveAlwaysRecreateFactory {
	return &HiveAlwaysRecreateFactory{hiveHost: hiveHost, hivePort: hivePort}
}

func (h *HiveAlwaysRecreateFactory) GetHive() (Hive, error) {
	clientHive, err := hmsclient.Open(h.hiveHost, h.hivePort)
	if err != nil {
		return nil, err
	}
	return clientHive, nil
}

type HiveMetaStore struct {
	hiveFactory HiveFactory
	fileDeleter deleter.FileDeleter
	aux         AuxInfoRetriever
}

func NewHiveMetaStore(hiveFactory HiveFactory, fileDeleter deleter.FileDeleter, aux AuxInfoRetriever) *HiveMetaStore {
	return &HiveMetaStore{hiveFactory: hiveFactory, fileDeleter: fileDeleter, aux: aux}
}

func (h *HiveMetaStore) GetTables(dbName string) ([]string, error) {
	hive, err := h.hiveFactory.GetHive()
	if err != nil {
		return nil, err
	}
	defer hive.Close()
	return hive.GetAllTables(dbName)
}

func (h *HiveMetaStore) GetTableInfo(dbName, tableName string) (model.TableInfo, error) {
	hive, err := h.hiveFactory.GetHive()
	if err != nil {
		return model.TableInfo{}, err
	}
	defer hive.Close()
	table, err := hive.GetTable(dbName, tableName)
	if err != nil {
		return model.TableInfo{}, err
	}
	format := model.FromInputOutput(table.Sd.InputFormat)
	location := table.Sd.Location
	if format == model.ICEBERG {
		location, err = h.aux.GetTableProperty(context.Background(), tableName, "metadata_location")
		if err != nil {
			return model.TableInfo{}, err
		}
	}
	return model.TableInfo{
		Name:             table.GetTableName(),
		Columns:          mapColumnsHive(table.Sd.Cols),
		Partitions:       mapColumnsHive(table.PartitionKeys),
		MetadataLocation: location,
		Format:           format,
	}, nil
}

func (h *HiveMetaStore) CreateTable(dbName string, table model.TableInfo) error {
	if len(table.Columns) == 0 {
		return fmt.Errorf("cannot Create table with 0 columns")
	}
	hive, err := h.hiveFactory.GetHive()
	if err != nil {
		return err
	}
	defer hive.Close()
	return hive.CreateTable(&hive_metastore.Table{
		TableName: table.Name,
		DbName:    dbName,
		Owner:     "metaman",
		Sd: &hive_metastore.StorageDescriptor{
			Cols:         unmapColumnsHive(table.Columns),
			Location:     getMetadataLocation(HIVE, table),
			InputFormat:  table.Format.InputFormat(),
			OutputFormat: table.Format.OutputFormat(),
			SerdeInfo:    mapSerdeInfoHive(table.Format.SerDeInfo()),
		},
		PartitionKeys: unmapColumnsHive(table.Partitions),
		Parameters:    table.Format.Parameters(convertS3Format(HIVE, table.MetadataLocation)),
		TableType:     table.Format.TableType(),
	})
}

func (h *HiveMetaStore) DropTable(dbName string, tableName string, deleteData bool) error {
	hive, err := h.hiveFactory.GetHive()
	if err != nil {
		return err
	}
	defer hive.Close()
	info, err := h.GetTableInfo(dbName, tableName)
	if err != nil {
		if _, ok := err.(*hive_metastore.NoSuchObjectException); ok {
			return nil
		}
		return err
	}
	err = hive.DropTable(dbName, tableName, deleteData)
	if err != nil {
		return err
	}
	if deleteData {
		if isOnS3(info.MetadataLocation) {
			bucket, path := getBucketPath(info.MetadataLocation)
			err := h.fileDeleter.Delete(context.Background(), bucket, path)
			if err != nil {
				logrus.Errorf("table dropped on hiveFactory but could not delete files if they are on s3")
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
			Type: mapHiveColumnType(model.UnmapColumnType(column.Type)),
		}
	}
	return cols
}

func mapHiveColumnType(columnType string) string {
	if strings.ToLower(columnType) == "string" {
		return "varchar(1024)"
	}
	return columnType
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
