package metastore

import (
	"github.com/stretchr/testify/require"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
	"testing"
)

type NamedMetastoreMock struct {
	name string
}

func (m *NamedMetastoreMock) GetTables(dbName string) ([]string, error) {
	return nil, nil
}

func (m *NamedMetastoreMock) GetTableInfo(dbName, tableName string) (model.TableInfo, error) {
	return model.TableInfo{}, nil
}

func (m *NamedMetastoreMock) CreateTable(dbName string, table model.TableInfo) error {
	return nil
}

func (m *NamedMetastoreMock) DropTable(dbName string, tableName string, deleteData bool) error {
	return nil
}

func TestPoolMetastore_Get(t *testing.T) {
	type args struct {
		metastore MetastoreCode
	}
	type fields struct {
		hive Metastore
		glue Metastore
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "shouldGetHive",
			args: args{
				metastore: HIVE,
			},
			fields: fields{
				hive: &NamedMetastoreMock{name: "hiveFactory"},
				glue: &NamedMetastoreMock{name: "glue"},
			},
			want:    "hiveFactory",
			wantErr: false,
		},
		{
			name: "shouldGetGlue",
			args: args{
				metastore: GLUE,
			},
			fields: fields{
				hive: &NamedMetastoreMock{name: "hiveFactory"},
				glue: &NamedMetastoreMock{name: "glue"},
			},
			want:    "glue",
			wantErr: false,
		},
		{
			name: "shouldErrorWhenNoSupportedMetastore",
			args: args{
				metastore: "no",
			},
			fields: fields{
				hive: &NamedMetastoreMock{name: "hiveFactory"},
				glue: &NamedMetastoreMock{name: "glue"},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewPoolMetastore(tt.fields.hive, tt.fields.glue)
			got, err := f.Get(tt.args.metastore)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				mock := got.(*NamedMetastoreMock)
				require.Equal(t, tt.want, mock.name)
			}
		})
	}
}
