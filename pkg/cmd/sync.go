package cmd

import (
	"github.com/spf13/cobra"
	"github.com/the-Data-Appeal-Company/metaman/pkg/metastore"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "sync tables between metastore",
	Long: `sync tables between metastore in the given database,
		an option could be passed to also delete tables that exist only in the target metastore
		(default value false)`,
	RunE: sync,
}

var (
	sourceMetastore string
	targetMetastore string
	sourceTables    []string
	deleteTables    bool
)

func init() {
	syncCmd.Flags().StringVarP(&sourceMetastore, "source", "s", "", "source metastore")
	syncCmd.Flags().StringVarP(&targetMetastore, "target", "t", "", "target metastore")
	syncCmd.Flags().StringVarP(&database, "database", "d", "", "database name")
	syncCmd.Flags().StringSliceVarP(&sourceTables, "tables", "", []string{}, "list of tables to sync to target")
	syncCmd.Flags().BoolVar(&deleteTables, "delete-tables", false, "delete tables from target non existing in source")
}

func sync(cmd *cobra.Command, args []string) error {
	metaman, err := getMetastoreManager()
	if err != nil {
		return err
	}
	source, target, err := mapSyncCommands()
	if err != nil {
		return err
	}
	return metaman.Sync(source, target, database, sourceTables, deleteTables)
}

func mapSyncCommands() (metastore.MetastoreCode, metastore.MetastoreCode, error) {
	source, err := mapMetastoreCode(sourceMetastore)
	if err != nil {
		return "", "", err
	}
	target, err := mapMetastoreCode(targetMetastore)
	if err != nil {
		return "", "", err
	}
	return source, target, nil
}
