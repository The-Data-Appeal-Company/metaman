package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/require"
	"github.com/the-Data-Appeal-Company/metaman/pkg/metastore"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type ManagerMock struct {
	dropCalls   []model.DropApiRequest
	dropError   error
	createCalls []model.CreateApiRequest
	createError error
	syncCalls   []model.SyncApiRequest
	syncError   error
}

func (m *ManagerMock) Drop(metastore metastore.MetastoreCode, tables []model.DropArg) error {
	m.dropCalls = append(m.dropCalls, model.DropApiRequest{
		Metastore: string(metastore),
		Tables:    tables,
	})
	if m.dropError != nil {
		return m.dropError
	}
	return nil
}

func (m *ManagerMock) Create(metastore []metastore.MetastoreCode, tables []model.DatabaseTables) error {
	m.createCalls = append(m.createCalls, model.CreateApiRequest{
		Metastores: toStrings(metastore),
		Tables:     tables,
	})
	if m.createError != nil {
		return m.createError
	}
	return nil
}

func (m *ManagerMock) Sync(sourceMetastore metastore.MetastoreCode, targetMetastore metastore.MetastoreCode, dbName string, delete bool) error {
	m.syncCalls = append(m.syncCalls, model.SyncApiRequest{
		Source: string(sourceMetastore),
		Target: string(targetMetastore),
		DbName: dbName,
		Delete: delete,
	})
	if m.syncError != nil {
		return m.syncError
	}
	return nil
}

func TestApiHandler_shouldCreate(t *testing.T) {
	type args struct {
		mock    ManagerMock
		request model.CreateApiRequest
		wantErr bool
	}

	var result error
	result = multierror.Append(result, fmt.Errorf("error"))
	tests := []args{
		{
			mock:    ManagerMock{},
			request: getCreateApiRequest([]string{"hive", "glue"}),
		},
		{
			mock: ManagerMock{
				createError: result,
			},
			wantErr: true,
			request: getCreateApiRequest([]string{"hive", "glue"}),
		},
		{
			mock:    ManagerMock{},
			wantErr: true,
			request: getCreateApiRequest([]string{"no"}),
		},
	}

	for _, test := range tests {
		handler := ApiHandler{manager: &test.mock}
		router := handler.setupRouter()
		w := httptest.NewRecorder()
		marshal, err := json.Marshal(test.request)
		require.NoError(t, err)
		req, _ := http.NewRequest("POST", "/create", strings.NewReader(string(marshal)))
		router.ServeHTTP(w, req)

		if test.wantErr {
			if test.mock.createError != nil {
				require.Equal(t, w.Code, http.StatusInternalServerError)
			} else {
				require.Equal(t, w.Code, http.StatusBadRequest)
			}
		} else {
			require.Equal(t, w.Code, http.StatusOK)
		}
		if !test.wantErr || test.mock.createError != nil {
			require.Len(t, test.mock.createCalls, 1)
			require.Equal(t, test.mock.createCalls[0], test.request)
		} else {
			require.Len(t, test.mock.createCalls, 0)
		}
	}
}

func getCreateApiRequest(met []string) model.CreateApiRequest {
	return model.CreateApiRequest{
		Metastores: met,
		Tables: []model.DatabaseTables{
			{
				Db: "pls",
				Tables: []model.TableInfo{
					{
						Name: "table",
						Columns: []model.Column{
							{
								Name: "id",
								Type: model.ColumnType{
									SqlType: model.BIGINT,
								},
							},
							{
								Name: "user",
								Type: model.ColumnType{
									SqlType: model.VARCHAR,
									Length:  200,
								},
							},
						},
						MetadataLocation: "s3://bucket/table",
						Format:           model.PARQUET,
					},
				},
			},
		},
	}
}

func toStrings(codes []metastore.MetastoreCode) []string {
	toReturn := make([]string, len(codes))
	for i, code := range codes {
		toReturn[i] = string(code)
	}
	return toReturn
}
