package metastore

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	awsGlue "github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
	"github.com/stretchr/testify/require"
	"github.com/the-Data-Appeal-Company/metaman/pkg/deleter"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
	"testing"
)

type GlueMock struct {
	glueiface.GlueAPI
	createCalls []*glue.CreateTableInput
	deleteCalls []*glue.DeleteTableInput
}

func (g *GlueMock) GetTable(input *glue.GetTableInput) (*glue.GetTableOutput, error) {
	if *input.DatabaseName != "pls" || *input.Name != "table" {
		return nil, fmt.Errorf("error")
	}
	return &glue.GetTableOutput{
		Table: getTableData(input.DatabaseName),
	}, nil
}

func (g *GlueMock) GetTables(input *glue.GetTablesInput) (*glue.GetTablesOutput, error) {
	if *input.DatabaseName != "pls" {
		return nil, fmt.Errorf("error")
	}
	return &glue.GetTablesOutput{
		NextToken: nil,
		TableList: []*glue.TableData{
			getTableData(input.DatabaseName),
		},
	}, nil
}

func (g *GlueMock) CreateTable(input *glue.CreateTableInput) (*glue.CreateTableOutput, error) {
	g.createCalls = append(g.createCalls, input)
	if *input.DatabaseName != "pls" {
		return nil, fmt.Errorf("error")
	}
	return &glue.CreateTableOutput{}, nil
}

func (g *GlueMock) DeleteTable(input *glue.DeleteTableInput) (*glue.DeleteTableOutput, error) {
	g.deleteCalls = append(g.deleteCalls, input)
	if *input.DatabaseName != "pls" {
		return nil, fmt.Errorf("error")
	}
	return &glue.DeleteTableOutput{}, nil
}

func getTableData(dbName *string) *awsGlue.TableData {
	return &awsGlue.TableData{
		CatalogId:                     aws.String("11111111111"),
		DatabaseName:                  dbName,
		IsRegisteredWithLakeFormation: boolPtr(false),
		Name:                          aws.String("table"),
		Owner:                         aws.String("sap"),
		StorageDescriptor: &glue.StorageDescriptor{
			Columns: []*glue.Column{
				{
					Name: aws.String("id"),
					Type: aws.String("bigint"),
				},
				{
					Name: aws.String("sign"),
					Type: aws.String("smallint"),
				},
				{
					Name: aws.String("topic"),
					Type: aws.String("varchar(200)"),
				},
				{
					Name: aws.String("quantity"),
					Type: aws.String("int"),
				},
				{
					Name: aws.String("price"),
					Type: aws.String("double"),
				},
				{
					Name: aws.String("start_date"),
					Type: aws.String("date"),
				},
				{
					Name: aws.String("closed"),
					Type: aws.String("boolean"),
				},
				{
					Name: aws.String("time_stamp"),
					Type: aws.String("timestamp"),
				},
			},
			Compressed:   boolPtr(false),
			InputFormat:  aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
			Location:     aws.String("s3://bucket/table"),
			OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
		},
		TableType: aws.String("EXTERNAL_TABLE"),
	}
}

func TestGlueMetaStore_GetTables(t *testing.T) {
	type fields struct {
		glue glueiface.GlueAPI
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
				glue: &GlueMock{},
			},
			args: args{
				dbName: "pls",
			},
			want:    []string{"table"},
			wantErr: false,
		},
		{
			name: "shouldErrorWhenGlueError",
			fields: fields{
				glue: &GlueMock{},
			},
			args: args{
				dbName: "errdb",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GlueMetaStore{
				glue: tt.fields.glue,
			}
			got, err := g.GetTables(tt.args.dbName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGlueMetaStore_GetTableInfo(t *testing.T) {
	type fields struct {
		glue glueiface.GlueAPI
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
			name: "shouldGetTableInfo",
			fields: fields{
				glue: &GlueMock{},
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
				MetadataLocation: "s3://bucket/table",
				Format:           model.PARQUET,
			},
			wantErr: false,
		},
		{
			name: "shouldErrorWhenGlueError",
			fields: fields{
				glue: &GlueMock{},
			},
			args: args{
				dbName:    "errdb",
				tableName: "table",
			},
			want:    model.TableInfo{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GlueMetaStore{
				glue: tt.fields.glue,
			}
			got, err := g.GetTableInfo(tt.args.dbName, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTableInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGlueMetaStore_CreateTable(t *testing.T) {
	type fields struct {
		glue glueiface.GlueAPI
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
				glue: &GlueMock{},
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
					MetadataLocation: "s3:/bucket/table",
					Format:           model.PARQUET,
				},
			},
			wantErr: false,
		},
		{
			name: "shouldErrorWhenGlueError",
			fields: fields{
				glue: &GlueMock{},
			},
			args: args{
				dbName: "err",
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
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GlueMetaStore{
				glue: tt.fields.glue,
			}
			if err := g.CreateTable(tt.args.dbName, tt.args.table); (err != nil) != tt.wantErr {
				t.Errorf("CreateTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			mock := tt.fields.glue.(*GlueMock)
			require.Len(t, mock.createCalls, 1)
			require.Equal(t, tt.args.dbName, *mock.createCalls[0].DatabaseName)
			require.Equal(t, tt.args.table.Name, *mock.createCalls[0].TableInput.Name)
			require.Equal(t, tt.args.table.MetadataLocation, *mock.createCalls[0].TableInput.StorageDescriptor.Location)
			require.Equal(t, tt.args.table.Format.InputFormat(), *mock.createCalls[0].TableInput.StorageDescriptor.InputFormat)
			require.Equal(t, tt.args.table.Format.OutputFormat(), *mock.createCalls[0].TableInput.StorageDescriptor.OutputFormat)
			require.Len(t, mock.createCalls[0].TableInput.StorageDescriptor.Columns, 2)
			for i, column := range mock.createCalls[0].TableInput.StorageDescriptor.Columns {
				require.Equal(t, tt.args.table.Columns[i].Name, *column.Name)
				require.Equal(t, model.UnmapColumnType(tt.args.table.Columns[i].Type), *column.Type)
			}
		})
	}
}

func TestGlueMetaStore_DropTable(t *testing.T) {
	type fields struct {
		glue        glueiface.GlueAPI
		fileDeleter deleter.FileDeleter
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
			name: "shouldDropTable",
			fields: fields{
				glue:        &GlueMock{},
				fileDeleter: &MockFileDeleter{},
			},
			args: args{
				dbName:    "pls",
				tableName: "table",
			},
			wantErr: false,
		},
		{
			name: "shouldDropTableDeleteData",
			fields: fields{
				glue:        &GlueMock{},
				fileDeleter: &MockFileDeleter{},
			},
			args: args{
				dbName:     "pls",
				tableName:  "table",
				deleteData: true,
			},
			wantErr: false,
		},
		{
			name: "shouldErrorWhenGlueError",
			fields: fields{
				glue:        &GlueMock{},
				fileDeleter: &MockFileDeleter{},
			},
			args: args{
				dbName:    "err",
				tableName: "table",
			},
			wantErr: true,
		},
		{
			name: "shouldErrorWhenGetTableInfoError",
			fields: fields{
				glue:        &GlueMock{},
				fileDeleter: &MockFileDeleter{},
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
				glue: &GlueMock{},
				fileDeleter: &MockFileDeleter{
					err: fmt.Errorf("error"),
				},
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
			g := NewGlueMetaStore(tt.fields.glue, tt.fields.fileDeleter)
			if err := g.DropTable(tt.args.dbName, tt.args.tableName, tt.args.deleteData); (err != nil) != tt.wantErr {
				t.Errorf("DropTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			mock := tt.fields.glue.(*GlueMock)
			require.Len(t, mock.deleteCalls, 1)
			require.Equal(t, *mock.deleteCalls[0].DatabaseName, tt.args.dbName)
			require.Equal(t, *mock.deleteCalls[0].Name, tt.args.tableName)

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
