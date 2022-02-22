package metastore

import (
	"context"
	"fmt"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/stretchr/testify/require"
	"github.com/the-Data-Appeal-Company/metaman/pkg/deleter"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
	"testing"
)

type AuxMock struct {
}

func (a *AuxMock) GetTableProperty(_ context.Context, table, _ string) (string, error) {
	return fmt.Sprintf("s3://bucket/%s/metadata/hcidhihcid.json", table), nil
}

type MockFileDeleter struct {
	paths map[string][]string
	err   error
}

func (m *MockFileDeleter) Delete(_ context.Context, bucket, path string) error {
	if _, found := m.paths[bucket]; !found {
		m.paths = make(map[string][]string)
		m.paths[bucket] = []string{path}
	} else {
		m.paths[bucket] = append(m.paths[bucket], path)
	}
	if m.err != nil {
		return m.err
	}
	return nil
}

type DropCall struct {
	dbName     string
	tableName  string
	deleteData bool
}

type HiveMock struct {
	createCalls []*hive_metastore.Table
	dropCalls   []DropCall
}

func (h *HiveMock) GetAllTables(dbName string) ([]string, error) {
	if dbName == "emptydb" {
		return []string{}, nil
	}
	if dbName != "pls" {
		return nil, fmt.Errorf("NoSuchObject")
	}
	return []string{"table", "table2"}, nil
}

func (h *HiveMock) CreateTable(table *hive_metastore.Table) error {
	h.createCalls = append(h.createCalls, table)
	if table.GetDbName() != "pls" {
		return fmt.Errorf("could not Create table")
	}
	return nil
}

func (h *HiveMock) DropTable(dbName string, tableName string, deleteData bool) error {
	h.dropCalls = append(h.dropCalls, DropCall{
		dbName:     dbName,
		tableName:  tableName,
		deleteData: deleteData,
	})
	if dbName != "pls" || tableName == "table1" {
		return fmt.Errorf("could not drop table")
	}
	return nil
}

func (h *HiveMock) GetTable(dbName string, tableName string) (*hive_metastore.Table, error) {
	if dbName != "pls" || (tableName != "table" && tableName != "table1") {
		return nil, fmt.Errorf("NoSuchObject")
	}
	return &hive_metastore.Table{
		TableName:  "table",
		DbName:     "pls",
		Owner:      "sap",
		CreateTime: 1644329244,
		Sd: &hive_metastore.StorageDescriptor{
			Cols: []*hive_metastore.FieldSchema{
				{
					Name: "id",
					Type: "bigint",
				},
				{
					Name: "sign",
					Type: "smallint",
				},
				{
					Name: "topic",
					Type: "varchar(200)",
				},
				{
					Name: "quantity",
					Type: "int",
				},
				{
					Name: "price",
					Type: "double",
				},
				{
					Name: "start_date",
					Type: "date",
				},
				{
					Name: "closed",
					Type: "boolean",
				},
				{
					Name: "time_stamp",
					Type: "timestamp",
				},
			},
			Location:     "s3a://bucket/table",
			InputFormat:  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			OutputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
			SerdeInfo: &hive_metastore.SerDeInfo{
				Name:             "table",
				SerializationLib: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
				Parameters:       map[string]string{},
			},
			BucketCols: make([]string, 0),
			SortCols:   make([]*hive_metastore.Order, 0),
			Parameters: map[string]string{},
			SkewedInfo: &hive_metastore.SkewedInfo{
				SkewedColNames:             make([]string, 0),
				SkewedColValues:            make([][]string, 0),
				SkewedColValueLocationMaps: map[string]string{},
			},
			StoredAsSubDirectories: boolPtr(false),
		},
		PartitionKeys:  make([]*hive_metastore.FieldSchema, 0),
		Parameters:     map[string]string{},
		TableType:      "EXTERNAL_TABLE",
		RewriteEnabled: boolPtr(false),
	}, nil
}

func TestHiveMetaStore_GetTableInfo(t *testing.T) {
	type fields struct {
		hive Hive
	}
	type args struct {
		dbName    string
		tableName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    model.TableInfo
		wantErr bool
	}{
		{
			name: "shouldGetTable",
			fields: fields{
				hive: &HiveMock{},
			},
			args: args{
				dbName:    "pls",
				tableName: "table",
			},
			want: model.TableInfo{
				Name: "table",
				Columns: []model.Column{
					{
						Name: "id",
						Type: model.ColumnType{SqlType: model.BIGINT},
					},
					{
						Name: "sign",
						Type: model.ColumnType{SqlType: model.SMALLINT},
					},
					{
						Name: "topic",
						Type: model.ColumnType{
							SqlType: model.VARCHAR,
							Length:  200,
						},
					},
					{
						Name: "quantity",
						Type: model.ColumnType{SqlType: model.INTEGER},
					},
					{
						Name: "price",
						Type: model.ColumnType{SqlType: model.DOUBLE},
					},
					{
						Name: "start_date",
						Type: model.ColumnType{SqlType: model.DATE},
					},
					{
						Name: "closed",
						Type: model.ColumnType{SqlType: model.BOOLEAN},
					},
					{
						Name: "time_stamp",
						Type: model.ColumnType{SqlType: model.TIMESTAMP},
					},
				},
				MetadataLocation: "s3a://bucket/table",
				Format:           model.PARQUET,
			},
			wantErr: false,
		},
		{
			name: "shouldHandleError",
			fields: fields{
				hive: &HiveMock{},
			},
			args: args{
				dbName:    "aaa",
				tableName: "baba",
			},
			want:    model.TableInfo{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &HiveMetaStore{
				hive: tt.fields.hive,
			}
			got, err := h.GetTableInfo(tt.args.dbName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTableInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestHiveMetaStore_GetTables(t *testing.T) {
	type fields struct {
		hive Hive
	}
	type args struct {
		dbName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "shouldGetTables",
			fields: fields{
				hive: &HiveMock{},
			},
			args: args{
				dbName: "pls",
			},
			want: []string{
				"table",
				"table2",
			},
			wantErr: false,
		},
		{
			name: "shouldHandleEmptyDb",
			fields: fields{
				hive: &HiveMock{},
			},
			args: args{
				dbName: "emptydb",
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "shouldHandleError",
			fields: fields{
				hive: &HiveMock{},
			},
			args: args{
				dbName: "nodb",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &HiveMetaStore{
				hive: tt.fields.hive,
			}
			got, err := h.GetTables(tt.args.dbName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestHiveMetaStore_CreateTable(t *testing.T) {
	type fields struct {
		hive Hive
	}
	type args struct {
		dbName string
		table  model.TableInfo
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "shouldCreateTable",
			fields: fields{
				hive: &HiveMock{},
			},
			args: args{
				dbName: "pls",
				table: model.TableInfo{
					Name: "table",
					Columns: []model.Column{
						{
							Name: "id",
							Type: model.ColumnType{SqlType: model.BIGINT},
						},
						{
							Name: "name",
							Type: model.ColumnType{SqlType: model.VARCHAR, Length: 200},
						},
					},
					MetadataLocation: "s3://bucket/table",
					Format:           model.PARQUET,
				},
			},
			wantErr: false,
		},
		{
			name: "shouldErrorWhenMetastoreError",
			fields: fields{
				hive: &HiveMock{},
			},
			args: args{
				dbName: "errdb",
				table: model.TableInfo{
					Name: "table",
					Columns: []model.Column{
						{
							Name: "id",
							Type: model.ColumnType{SqlType: model.BIGINT},
						},
					},
					MetadataLocation: "s3://bucket/table",
				},
			},
			wantErr: true,
		},
		{
			name: "shouldErrorWhenNoColumnsSpecified",
			fields: fields{
				hive: &HiveMock{},
			},
			args: args{
				dbName: "pls",
				table: model.TableInfo{
					Name:             "table",
					Columns:          []model.Column{},
					MetadataLocation: "s3://bucket/table",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &HiveMetaStore{
				hive: tt.fields.hive,
			}
			if err := h.CreateTable(tt.args.dbName, tt.args.table); (err != nil) != tt.wantErr {
				t.Errorf("CreateTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			mock := tt.fields.hive.(*HiveMock)
			if len(tt.args.table.Columns) == 0 {
				require.Len(t, mock.createCalls, 0)
				return
			}
			require.Len(t, mock.createCalls, 1)
			require.Equal(t, mock.createCalls[0].TableName, tt.args.table.Name)
			require.Equal(t, mock.createCalls[0].DbName, tt.args.dbName)
			require.Equal(t, mock.createCalls[0].Sd.Location, tt.args.table.MetadataLocation)
			require.Equal(t, mock.createCalls[0].Sd.InputFormat, tt.args.table.Format.InputFormat())
			require.Equal(t, mock.createCalls[0].Sd.OutputFormat, tt.args.table.Format.OutputFormat())
			require.Len(t, mock.createCalls[0].Sd.Cols, len(tt.args.table.Columns))
			for i, column := range tt.args.table.Columns {
				require.Equal(t, mock.createCalls[0].Sd.Cols[i], &hive_metastore.FieldSchema{
					Name: column.Name,
					Type: model.UnmapColumnType(column.Type),
				})
			}
		})
	}
}

func TestHiveMetaStore_DropTable(t *testing.T) {
	type fields struct {
		hive        Hive
		fileDeleter deleter.FileDeleter
		aux         AuxInfoRetriever
	}
	type args struct {
		dbName     string
		tableName  string
		deleteData bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "shouldDropTableNoDeleteData",
			fields: fields{
				hive:        &HiveMock{},
				fileDeleter: &MockFileDeleter{},
				aux:         &AuxMock{},
			},
			args: args{
				dbName:     "pls",
				tableName:  "table",
				deleteData: false,
			},
			wantErr: false,
		},
		{
			name: "shouldDropTableDeleteData",
			fields: fields{
				hive:        &HiveMock{},
				fileDeleter: &MockFileDeleter{},
				aux:         &AuxMock{},
			},
			args: args{
				dbName:     "pls",
				tableName:  "table",
				deleteData: true,
			},
			wantErr: false,
		},
		{
			name: "shouldErrorWhenHiveError",
			fields: fields{
				hive:        &HiveMock{},
				fileDeleter: &MockFileDeleter{},
				aux:         &AuxMock{},
			},
			args: args{
				dbName:     "pls",
				tableName:  "table1",
				deleteData: false,
			},
			wantErr: true,
		},
		{
			name: "shouldErrorWhenGetTableInfoError",
			fields: fields{
				hive:        &HiveMock{},
				fileDeleter: &MockFileDeleter{},
				aux:         &AuxMock{},
			},
			args: args{
				dbName:     "pls",
				tableName:  "tab",
				deleteData: true,
			},
			wantErr: true,
		},
		{
			name: "shouldErrorWhenS3Error",
			fields: fields{
				hive: &HiveMock{},
				fileDeleter: &MockFileDeleter{
					err: fmt.Errorf("error"),
				},
				aux: &AuxMock{},
			},
			args: args{
				dbName:     "pls",
				tableName:  "table",
				deleteData: true,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHiveMetaStore(tt.fields.hive, tt.fields.fileDeleter, tt.fields.aux)
			if err := h.DropTable(tt.args.dbName, tt.args.tableName, tt.args.deleteData); (err != nil) != tt.wantErr {
				t.Errorf("DropTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			mock := tt.fields.hive.(*HiveMock)
			if tt.args.tableName == "tab" {
				require.Len(t, mock.dropCalls, 0)
			} else {
				require.Len(t, mock.dropCalls, 1)
				require.Equal(t, mock.dropCalls[0].dbName, tt.args.dbName)
				require.Equal(t, mock.dropCalls[0].tableName, tt.args.tableName)
				require.Equal(t, mock.dropCalls[0].deleteData, tt.args.deleteData)
			}
			fileDeleter := tt.fields.fileDeleter.(*MockFileDeleter)
			if tt.args.deleteData && (!tt.wantErr || fileDeleter.err != nil) {
				require.Len(t, fileDeleter.paths["bucket"], 1)
				require.Equal(t, "table", fileDeleter.paths["bucket"][0])
			} else {
				require.Nil(t, fileDeleter.paths["bucket"])
			}
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}
