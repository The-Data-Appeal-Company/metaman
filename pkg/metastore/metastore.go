package metastore

import (
	"fmt"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
)

type MetastoreCode string

const (
	HIVE MetastoreCode = "hive"
	GLUE MetastoreCode = "glue"
)

type Metastore interface {
	GetTables(dbName string) ([]string, error)
	GetTableInfo(dbName, tableName string) (model.TableInfo, error)
	CreateTable(dbName string, table model.TableInfo) error
	DropTable(dbName string, tableName string, deleteData bool) error
}

type Pool interface {
	Get(metastore MetastoreCode) (Metastore, error)
}

type PoolMetastore struct {
	hive Metastore
	glue Metastore
}

func NewPoolMetastore(hive Metastore, glue Metastore) *PoolMetastore {
	return &PoolMetastore{hive: hive, glue: glue}
}

func (f *PoolMetastore) Get(metastore MetastoreCode) (Metastore, error) {
	switch metastore {
	case GLUE:
		return f.glue, nil
	case HIVE:
		return f.hive, nil
	default:
		return nil, fmt.Errorf("could not get '%s' metastore", metastore)
	}
}
