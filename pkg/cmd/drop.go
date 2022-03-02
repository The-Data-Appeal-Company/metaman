package cmd

import (
	"github.com/hashicorp/go-multierror"
	"github.com/spf13/cobra"
	"github.com/the-Data-Appeal-Company/metaman/pkg/metastore"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
)

var dropCmd = &cobra.Command{
	Use:   "drop",
	Short: "drop table",
	Long:  `drop table`,
	RunE:  drop,
}

var (
	metastoreName string
	database      string
	tables        []string
	deleteData    bool
)

func init() {
	dropCmd.Flags().StringVarP(&metastoreName, "metastore", "m", "", "metastore")
	dropCmd.Flags().StringVarP(&database, "database", "d", "", "database name")
	dropCmd.Flags().StringArray("tables", tables, "list of table names")
	dropCmd.Flags().BoolVar(&deleteData, "delete-data", false, "delete table data")
}

func drop(cmd *cobra.Command, args []string) error {
	metaman, err := getMetastoreManager()
	if err != nil {
		return err
	}
	code, tables, err := mapDropCommands()
	if err != nil {
		return err
	}
	errors := metaman.Drop(code, tables)
	var result error
	for _, err = range errors {
		result = multierror.Append(result, err)
	}
	return result
}

func mapDropCommands() (metastore.MetastoreCode, []model.DropArg, error) {
	code, err := mapMetastoreCode(metastoreName)
	if err != nil {
		return "", nil, err
	}
	args := make([]model.DropArg, 1)
	args[0] = model.DropArg{
		Db:     database,
		Tables: make([]model.DropTable, len(tables)),
	}
	for i, table := range tables {
		args[0].Tables[i] = model.DropTable{
			Table:      table,
			DeleteData: deleteData,
		}
	}
	return code, args, nil
}
