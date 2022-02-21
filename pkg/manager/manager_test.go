package manager

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/the-Data-Appeal-Company/metaman/pkg/metastore"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
	"testing"
)

type MetastoreMock struct {
	getTablesCalls       []string
	getTableInfoCalls    map[string][]string
	createTableInfoCalls []model.DatabaseTables
	dropTableInfoCalls   []model.DropArg
	getTablesOut         []string
	getTablesError       error
	getTableInfoError    map[string]error
	createTableError     map[string]error
	dropTableError       map[string]error
}

func (m *MetastoreMock) GetTables(dbName string) ([]string, error) {
	m.getTablesCalls = append(m.getTablesCalls, dbName)
	if m.getTablesError != nil {
		return nil, m.getTablesError
	}
	if dbName != "pls" {
		return nil, fmt.Errorf("error")
	}
	return m.getTablesOut, nil
}

func (m *MetastoreMock) GetTableInfo(dbName, tableName string) (model.TableInfo, error) {
	if _, found := m.getTableInfoCalls[dbName]; !found {
		m.getTableInfoCalls = make(map[string][]string)
		m.getTableInfoCalls[dbName] = []string{tableName}
	} else {
		m.getTableInfoCalls[dbName] = append(m.getTableInfoCalls[dbName], tableName)
	}
	err := m.getTableInfoError[tableName]
	if err != nil {
		return model.TableInfo{}, err
	}
	if tableName == "table2" {
		return model.TableInfo{}, fmt.Errorf("error")
	}
	return getTableInfo(tableName), nil
}

func (m *MetastoreMock) CreateTable(dbName string, table model.TableInfo) error {
	idx := -1
	for i, call := range m.createTableInfoCalls {
		if call.Db == dbName {
			idx = i
			break
		}
	}
	if idx < 0 {
		m.createTableInfoCalls = append(m.createTableInfoCalls, model.DatabaseTables{
			Db:     dbName,
			Tables: []model.TableInfo{table},
		})
	} else {
		m.createTableInfoCalls[idx].Tables = append(m.createTableInfoCalls[idx].Tables, table)
	}
	err := m.createTableError[table.Name]
	if err != nil {
		return err
	}
	if table.Name == "table2" {
		return fmt.Errorf("error")
	}
	return nil
}

func (m *MetastoreMock) DropTable(dbName string, tableName string, deleteData bool) error {
	idx := -1
	for i, call := range m.dropTableInfoCalls {
		if call.Db == dbName {
			idx = i
			break
		}
	}
	table := model.DropTable{
		Table:      tableName,
		DeleteData: deleteData,
	}
	if idx < 0 {
		m.dropTableInfoCalls = append(m.dropTableInfoCalls, model.DropArg{
			Db:     dbName,
			Tables: []model.DropTable{table},
		})
	} else {
		m.dropTableInfoCalls[idx].Tables = append(m.dropTableInfoCalls[idx].Tables, table)
	}
	err := m.dropTableError[tableName]
	if err != nil {
		return err
	}
	if tableName == "table2" {
		return fmt.Errorf("error")
	}
	return nil
}

type MockPool struct {
	hive *MetastoreMock
	glue *MetastoreMock
}

func NewMockPool() *MockPool {
	return &MockPool{hive: &MetastoreMock{}, glue: &MetastoreMock{}}
}

func (m *MockPool) Get(met metastore.MetastoreCode) (metastore.Metastore, error) {
	if met == metastore.HIVE {
		return m.hive, nil
	}
	if met == metastore.GLUE {
		return m.glue, nil
	}
	return nil, fmt.Errorf("no such metastore")
}

func TestHiveGlueManager_Drop(t *testing.T) {
	type fields struct {
		pool metastore.Pool
	}
	type args struct {
		metastore metastore.MetastoreCode
		tables    []model.DropArg
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "shouldErrorWhenNoValidMetastore",
			fields: fields{
				pool: NewMockPool(),
			},
			args: args{
				metastore: "no",
				tables: []model.DropArg{
					{
						Db: "pls",
						Tables: []model.DropTable{
							{
								Table:      "Table",
								DeleteData: true,
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "shouldDropTable",
			fields: fields{
				pool: NewMockPool(),
			},
			args: args{
				metastore: metastore.GLUE,
				tables: []model.DropArg{
					{
						Db: "pls",
						Tables: []model.DropTable{
							{
								Table:      "Table",
								DeleteData: true,
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "shouldTryToDropAllTablesAndGatherAllErrors",
			fields: fields{
				pool: NewMockPool(),
			},
			args: args{
				metastore: metastore.GLUE,
				tables: []model.DropArg{
					{
						Db: "pls",
						Tables: []model.DropTable{
							{
								Table:      "Table",
								DeleteData: false,
							},
							{
								Table:      "table2",
								DeleteData: false,
							},
							{
								Table:      "table3",
								DeleteData: true,
							},
						},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHiveGlueManager(tt.fields.pool)
			if err := h.Drop(tt.args.metastore, tt.args.tables); (err != nil) != tt.wantErr {
				t.Errorf("Drop() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.args.metastore == "no" {
				return
			}
			mock := getMetastore(tt.fields.pool.(*MockPool), tt.args.metastore)

			require.Len(t, mock.dropTableInfoCalls, 1)
			require.Equal(t, mock.dropTableInfoCalls[0].Db, "pls")
			require.Len(t, mock.dropTableInfoCalls[0].Tables, len(tt.args.tables[0].Tables))
			for i, call := range mock.dropTableInfoCalls[0].Tables {
				require.Equal(t, call, tt.args.tables[0].Tables[i])
			}
		})
	}
}

func TestHiveGlueManager_Create(t *testing.T) {
	type fields struct {
		pool metastore.Pool
	}
	type args struct {
		metastore []metastore.MetastoreCode
		tables    []model.DatabaseTables
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
				pool: NewMockPool(),
			},
			args: args{
				metastore: []metastore.MetastoreCode{metastore.HIVE, metastore.GLUE},
				tables: []model.DatabaseTables{
					{
						Db: "pls", Tables: []model.TableInfo{getTableInfo("Table")},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "shouldTryToCreateAllTablesAndGatherAllErrors",
			fields: fields{
				pool: NewMockPool(),
			},
			args: args{
				metastore: []metastore.MetastoreCode{metastore.HIVE, metastore.GLUE},
				tables: []model.DatabaseTables{
					{
						Db: "pls", Tables: []model.TableInfo{
							getTableInfo("Table"),
							getTableInfo("table2"),
							getTableInfo("table3"),
						},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &HiveGlueManager{
				pool: tt.fields.pool,
			}
			if err := h.Create(tt.args.metastore, tt.args.tables); (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for _, met := range tt.args.metastore {
				mock := getMetastore(tt.fields.pool.(*MockPool), met)
				require.Len(t, mock.createTableInfoCalls, len(tt.args.tables))
				for i := range mock.createTableInfoCalls {
					require.Equal(t, mock.createTableInfoCalls[i].Db, "pls")
					require.Len(t, mock.createTableInfoCalls[i].Tables, len(tt.args.tables[i].Tables))
					for j, call := range mock.createTableInfoCalls[i].Tables {
						require.Equal(t, call, tt.args.tables[i].Tables[j%len(tt.args.tables[i].Tables)])
					}
				}
			}
		})
	}
}

func TestCreateContinueIfOneOfTHeMetastoreDoesNotExist(t *testing.T) {
	pool := NewMockPool()
	mock := pool.hive
	h := &HiveGlueManager{
		pool: pool,
	}
	require.Error(t, h.Create([]metastore.MetastoreCode{"no", metastore.HIVE},
		[]model.DatabaseTables{
			{
				Db: "pls", Tables: []model.TableInfo{
					getTableInfo("Table"),
					getTableInfo("table3"),
				},
			},
		}),
	)

	require.Len(t, mock.createTableInfoCalls, 1)
	require.Equal(t, mock.createTableInfoCalls[0].Db, "pls")
	require.Len(t, mock.createTableInfoCalls[0].Tables, 2)

	require.Equal(t, mock.createTableInfoCalls[0].Tables[0], getTableInfo("Table"))
	require.Equal(t, mock.createTableInfoCalls[0].Tables[1], getTableInfo("table3"))
}

func TestHiveGlueManager_SyncErrorNonExistingSourceMetastore(t *testing.T) {
	h := &HiveGlueManager{
		pool: NewMockPool(),
	}
	require.Error(t, h.Sync("no", metastore.GLUE, "pls", nil, false))
}

func TestHiveGlueManager_SyncErrorNonExistingTargetMetastore(t *testing.T) {
	h := &HiveGlueManager{
		pool: NewMockPool(),
	}
	require.Error(t, h.Sync(metastore.GLUE, "no", "pls", nil, false))
}

func TestHiveGlueManager_SyncErrorWhenSourceGetTablesError(t *testing.T) {
	h := &HiveGlueManager{
		pool: NewMockPool(),
	}
	require.Error(t, h.Sync(metastore.GLUE, metastore.HIVE, "err", nil, false))
}

func TestHiveGlueManager_SyncErrorWhenTargetGetTablesError(t *testing.T) {
	h := &HiveGlueManager{
		pool: &MockPool{hive: &MetastoreMock{}, glue: &MetastoreMock{getTablesError: fmt.Errorf("error")}},
	}
	require.Error(t, h.Sync(metastore.HIVE, metastore.GLUE, "pls", nil, false))
}

func TestHiveGlueManager_SyncNoDifferences(t *testing.T) {
	pool := NewMockPool()
	h := &HiveGlueManager{
		pool: pool,
	}
	require.NoError(t, h.Sync(metastore.HIVE, metastore.GLUE, "pls", nil, false))

	require.Len(t, pool.glue.createTableInfoCalls, 0)
}

func TestHiveGlueManager_SyncOnlyCreate(t *testing.T) {
	pool := &MockPool{hive: &MetastoreMock{getTablesOut: []string{"tab1", "tab2", "tab3"}}, glue: &MetastoreMock{getTablesOut: []string{"tab1"}}}
	h := &HiveGlueManager{
		pool: pool,
	}
	require.NoError(t, h.Sync(metastore.HIVE, metastore.GLUE, "pls", nil, false))

	require.Len(t, pool.glue.createTableInfoCalls, 1)
	require.Equal(t, pool.glue.createTableInfoCalls[0].Db, "pls")
	require.Len(t, pool.glue.createTableInfoCalls[0].Tables, 2)
}

func TestHiveGlueManager_SyncTargetHasMoreThanSource(t *testing.T) {
	pool := &MockPool{hive: &MetastoreMock{getTablesOut: []string{"tab1", "tab2", "tab3"}}, glue: &MetastoreMock{getTablesOut: []string{"tab1"}}}
	h := &HiveGlueManager{
		pool: pool,
	}
	require.NoError(t, h.Sync(metastore.GLUE, metastore.HIVE, "pls", nil, false))

	require.Len(t, pool.glue.createTableInfoCalls, 0)
}

func TestHiveGlueManager_SyncContinueOnGetTableInfoError(t *testing.T) {
	pool := &MockPool{hive: &MetastoreMock{getTablesOut: []string{"tab1", "tab2", "tab3"}, getTableInfoError: map[string]error{
		"tab2": fmt.Errorf("error"),
	}}, glue: &MetastoreMock{getTablesOut: []string{}}}
	h := &HiveGlueManager{
		pool: pool,
	}
	require.Error(t, h.Sync(metastore.HIVE, metastore.GLUE, "pls", nil, false))

	require.Len(t, pool.glue.createTableInfoCalls, 1)
	require.Equal(t, pool.glue.createTableInfoCalls[0].Db, "pls")
	require.Len(t, pool.glue.createTableInfoCalls[0].Tables, 2)
}

func TestHiveGlueManager_SyncContinueOnCreateTableError(t *testing.T) {
	pool := &MockPool{hive: &MetastoreMock{getTablesOut: []string{"tab1", "tab2", "tab3"}}, glue: &MetastoreMock{getTablesOut: []string{}, createTableError: map[string]error{
		"tab2": fmt.Errorf("error"),
	}}}
	h := &HiveGlueManager{
		pool: pool,
	}
	require.Error(t, h.Sync(metastore.HIVE, metastore.GLUE, "pls", nil, false))

	require.Len(t, pool.glue.createTableInfoCalls, 1)
	require.Equal(t, pool.glue.createTableInfoCalls[0].Db, "pls")
	require.Len(t, pool.glue.createTableInfoCalls[0].Tables, 3)
}

func TestHiveGlueManager_SyncContinueOnDropTableError(t *testing.T) {
	pool := &MockPool{hive: &MetastoreMock{getTablesOut: []string{}}, glue: &MetastoreMock{getTablesOut: []string{"tab1", "tab2", "tab3"}, dropTableError: map[string]error{
		"tab2": fmt.Errorf("error"),
	}}}
	h := &HiveGlueManager{
		pool: pool,
	}
	require.Error(t, h.Sync(metastore.HIVE, metastore.GLUE, "pls", nil, true))

	require.Len(t, pool.glue.dropTableInfoCalls, 1)
	require.Equal(t, pool.glue.dropTableInfoCalls[0].Db, "pls")
	require.Len(t, pool.glue.dropTableInfoCalls[0].Tables, 3)
}

func TestHiveGlueManager_SyncNoDelete(t *testing.T) {
	pool := &MockPool{hive: &MetastoreMock{getTablesOut: []string{"tab1", "tab2"}}, glue: &MetastoreMock{getTablesOut: []string{"tab3", "tab1"}}}
	h := &HiveGlueManager{
		pool: pool,
	}
	require.NoError(t, h.Sync(metastore.HIVE, metastore.GLUE, "pls", nil, false))

	require.Len(t, pool.glue.createTableInfoCalls, 1)
	require.Equal(t, pool.glue.createTableInfoCalls[0].Db, "pls")
	require.Len(t, pool.glue.createTableInfoCalls[0].Tables, 1)
	require.Len(t, pool.glue.dropTableInfoCalls, 0)
}

func TestHiveGlueManager_SyncDelete(t *testing.T) {
	pool := &MockPool{hive: &MetastoreMock{getTablesOut: []string{"tab1", "tab2"}}, glue: &MetastoreMock{getTablesOut: []string{"tab3", "tab1"}}}
	h := &HiveGlueManager{
		pool: pool,
	}
	require.NoError(t, h.Sync(metastore.HIVE, metastore.GLUE, "pls", nil, true))

	require.Len(t, pool.glue.createTableInfoCalls, 1)
	require.Equal(t, pool.glue.createTableInfoCalls[0].Db, "pls")
	require.Len(t, pool.glue.createTableInfoCalls[0].Tables, 1)

	require.Len(t, pool.glue.dropTableInfoCalls, 1)
	require.Equal(t, pool.glue.dropTableInfoCalls[0].Db, "pls")
	require.Len(t, pool.glue.dropTableInfoCalls[0].Tables, 1)
}

func getTableInfo(table string) model.TableInfo {
	return model.TableInfo{
		Name: table,
		Columns: []model.Column{
			{
				Name: "id",
				Type: model.ColumnType{SqlType: model.BIGINT},
			},
		},
		MetadataLocation: fmt.Sprintf("s3://bucket/%s", table),
		Format:           model.PARQUET,
	}
}

func getMetastore(pool *MockPool, met metastore.MetastoreCode) *MetastoreMock {
	var mock *MetastoreMock
	if met == metastore.GLUE {
		mock = pool.glue
	}
	if met == metastore.HIVE {
		mock = pool.hive
	}
	return mock
}
