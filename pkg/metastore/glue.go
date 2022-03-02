package metastore

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
	"github.com/sirupsen/logrus"
	"github.com/the-Data-Appeal-Company/metaman/pkg/deleter"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
)

type GlueMetaStore struct {
	glue        glueiface.GlueAPI
	fileDeleter deleter.FileDeleter
}

func NewGlueMetaStore(glue glueiface.GlueAPI, fileDeleter deleter.FileDeleter) *GlueMetaStore {
	return &GlueMetaStore{glue: glue, fileDeleter: fileDeleter}
}

func (g *GlueMetaStore) GetTables(dbName string) ([]string, error) {
	tables, err := g.glue.GetTables(&glue.GetTablesInput{
		DatabaseName: &dbName,
	})
	if err != nil {
		return nil, err
	}
	ts := make([]string, len(tables.TableList))
	for i, table := range tables.TableList {
		ts[i] = *table.Name
	}
	return ts, nil
}

func (g *GlueMetaStore) GetTableInfo(dbName, tableName string) (model.TableInfo, error) {
	table, err := g.glue.GetTable(&glue.GetTableInput{
		DatabaseName: &dbName,
		Name:         &tableName,
	})
	if err != nil {
		return model.TableInfo{}, err
	}
	return model.TableInfo{
		Name:             stringFromPtr(table.Table.Name),
		Columns:          mapColumnsGlue(table.Table.StorageDescriptor.Columns),
		MetadataLocation: stringFromPtr(table.Table.StorageDescriptor.Location),
		Format:           model.FromInputOutput(stringFromPtr(table.Table.StorageDescriptor.InputFormat)),
	}, nil
}

func (g *GlueMetaStore) CreateTable(dbName string, table model.TableInfo) error {
	_, err := g.glue.CreateTable(&glue.CreateTableInput{
		DatabaseName: &dbName,
		TableInput: &glue.TableInput{
			Name: &table.Name,
			StorageDescriptor: &glue.StorageDescriptor{
				Columns:      unmapColumnsGlue(table.Columns),
				InputFormat:  aws.String(table.Format.InputFormat()),
				Location:     aws.String(getMetadataLocation(GLUE, table)),
				OutputFormat: aws.String(table.Format.OutputFormat()),
				SerdeInfo:    mapSerdeInfoGlue(table.Format.SerDeInfo()),
			},
			TableType:  table.Format.TableType(),
			Parameters: mapParametersGlue(table.Format.Parameters(convertS3Format(GLUE, table.MetadataLocation))),
		},
	})
	return err
}

func (g *GlueMetaStore) DropTable(dbName string, tableName string, deleteData bool) error {
	info, err := g.GetTableInfo(dbName, tableName)
	if err != nil {
		if _, ok := err.(*glue.EntityNotFoundException); ok {
			return nil
		}
		return err
	}
	_, err = g.glue.DeleteTable(&glue.DeleteTableInput{
		DatabaseName: aws.String(dbName),
		Name:         aws.String(tableName),
	})
	if err != nil {
		return err
	}
	if deleteData {
		if isOnS3(info.MetadataLocation) {
			bucket, path := getBucketPath(info.MetadataLocation)
			err := g.fileDeleter.Delete(context.Background(), bucket, path)
			if err != nil {
				logrus.Errorf("table dropped on glue but could not delete files if they are on s3")
				return err
			}
		}
	}
	return nil
}

func mapColumnsGlue(columns []*glue.Column) []model.Column {
	cols := make([]model.Column, len(columns))
	for i, column := range columns {
		cols[i] = model.Column{
			Name: stringFromPtr(column.Name),
			Type: model.MapColumnType(stringFromPtr(column.Type)),
		}
	}
	return cols
}

func unmapColumnsGlue(columns []model.Column) []*glue.Column {
	cols := make([]*glue.Column, len(columns))
	for i := range columns {
		cols[i] = &glue.Column{
			Name: &columns[i].Name,
			Type: aws.String(model.UnmapColumnType(columns[i].Type)),
		}
	}
	return cols
}

func mapParametersGlue(parameters map[string]string) map[string]*string {
	params := make(map[string]*string)
	for k, v := range parameters {
		tmp := v
		params[k] = &tmp
	}
	return params
}

func mapSerdeInfoGlue(info *model.SerDeInfo) *glue.SerDeInfo {
	if info == nil {
		return nil
	}
	return &glue.SerDeInfo{
		SerializationLibrary: aws.String(info.SerializationLib),
		Parameters:           mapParametersGlue(info.Parameters),
	}
}
