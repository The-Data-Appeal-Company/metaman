package manager

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
	"github.com/the-Data-Appeal-Company/metaman/pkg/metastore"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
)

type Manager interface {
	Drop(metastore metastore.MetastoreCode, tables []model.DropArg) []error
	Create(metastore []metastore.MetastoreCode, tables []model.DatabaseTables) error
	Sync(sourceMetastore metastore.MetastoreCode, targetMetastore metastore.MetastoreCode, dbName string, tables []string, delete bool) error
}

type HiveGlueManager struct {
	pool metastore.Pool
}

func NewHiveGlueManager(pool metastore.Pool) *HiveGlueManager {
	return &HiveGlueManager{pool: pool}
}

func (h *HiveGlueManager) Drop(metastore metastore.MetastoreCode, tables []model.DropArg) []error {
	meta, err := h.pool.Get(metastore)
	if err != nil {
		return []error{err}
	}
	var errors []error
	for _, dbTab := range tables {
		for _, tab := range dbTab.Tables {
			logrus.Infof("drop table: %s", tab.Table)
			err = meta.DropTable(dbTab.Db, tab.Table, tab.DeleteData)
			if err != nil {
				errors = append(errors, fmt.Errorf("db: %s, table: %s, error: %s", dbTab.Db, tab.Table, err.Error()))
			}
		}
	}
	return errors
}

func (h *HiveGlueManager) Create(metastores []metastore.MetastoreCode, tables []model.DatabaseTables) error {
	var result error
	for _, code := range metastores {
		meta, err := h.pool.Get(code)
		if err != nil {
			result = multierror.Append(result, err)
			continue
		}
		for _, dbTab := range tables {
			db := dbTab.Db
			for _, tab := range dbTab.Tables {
				logrus.Infof("create table: %s", tab.Name)
				err := meta.CreateTable(db, tab)
				if err != nil {
					result = multierror.Append(result, err)
				}
			}
		}
	}
	return result
}

func (h *HiveGlueManager) Sync(sourceMetastore metastore.MetastoreCode, targetMetastore metastore.MetastoreCode, dbName string, tables []string, delete bool) error {
	source, err := h.pool.Get(sourceMetastore)
	if err != nil {
		return err
	}
	target, err := h.pool.Get(targetMetastore)
	if err != nil {
		return err
	}
	logrus.Infof("syncing tables from: %s to: %s, db: %s", sourceMetastore, targetMetastore, dbName)
	sourceTables := tables
	if len(tables) == 0 {
		sourceTables, err = source.GetTables(dbName)
		if err != nil {
			return err
		}
	}
	targetTables, err := target.GetTables(dbName)
	if err != nil {
		return err
	}
	//create
	result := syncTables(source, target, dbName, sourceTables, targetTables)
	//drop
	if delete {
		for _, targetTable := range targetTables {
			if !tableExists(targetTable, sourceTables) {
				logrus.Infof("drop table: %s", targetTable)
				err := target.DropTable(dbName, targetTable, delete)
				if err != nil {
					result = multierror.Append(result, err)
				}
			}
		}
	}
	return result
}

func syncTables(source metastore.Metastore, target metastore.Metastore, dbName string, sourceTables, targetTables []string) error {
	var result error
	for _, sourceTable := range sourceTables {
		if !tableExists(sourceTable, targetTables) {
			logrus.Infof("create table: %s", sourceTable)
			err := createTable(source, target, dbName, sourceTable)
			if err != nil {
				result = multierror.Append(result, err)
			}
		}
	}
	return result
}

func createTable(source metastore.Metastore, target metastore.Metastore, dbName string, sourceTable string) error {
	info, err := source.GetTableInfo(dbName, sourceTable)
	if err != nil {
		return err
	}
	err = target.CreateTable(dbName, info)
	if err != nil {
		return err
	}
	return nil
}

func tableExists(sourceTable string, targetTables []string) bool {
	for _, targetTable := range targetTables {
		if targetTable == sourceTable {
			return true
		}
	}
	return false
}
