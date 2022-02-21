package cmd

import (
	"encoding/json"
	"github.com/spf13/cobra"
	"github.com/the-Data-Appeal-Company/metaman/pkg/metastore"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
	"io/ioutil"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "create tables",
	Long:  `create tables reading definition from a json file in the given database`,
	RunE:  create,
}

var (
	metastoreNames       []string
	tablesDefinitionPath string
)

func init() {
	createCmd.Flags().StringArray("metastores", metastoreNames, "list of metastore")
	createCmd.Flags().StringVarP(&database, "database", "d", "", "database name")
	createCmd.Flags().StringVarP(&tablesDefinitionPath, "tables-definition", "t", "", "path to json with tables definition")
}

func create(cmd *cobra.Command, args []string) error {
	metaman, err := getMetastoreManager()
	if err != nil {
		return err
	}
	codes, tables, err := mapCreateCommands()
	if err != nil {
		return err
	}
	return metaman.Create(codes, tables)
}

func mapCreateCommands() ([]metastore.MetastoreCode, []model.DatabaseTables, error) {
	codes, err := mapMetastoreCodes(metastoreNames)
	if err != nil {
		return nil, nil, err
	}
	file, err := ioutil.ReadFile(tablesDefinitionPath)
	if err != nil {
		return nil, nil, err
	}
	var args []model.DatabaseTables
	err = json.Unmarshal(file, &args)
	if err != nil {
		return nil, nil, err
	}
	return codes, args, nil
}

func mapMetastoreCodes(names []string) ([]metastore.MetastoreCode, error) {
	codes := make([]metastore.MetastoreCode, len(names))
	for i, name := range names {
		code, err := mapMetastoreCode(name)
		if err != nil {
			return nil, err
		}
		codes[i] = code
	}
	return codes, nil
}
