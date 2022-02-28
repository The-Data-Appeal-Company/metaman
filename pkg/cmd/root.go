package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/akolb1/gometastore/hmsclient"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsGlue "github.com/aws/aws-sdk-go/service/glue"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	metamanConf "github.com/the-Data-Appeal-Company/metaman/pkg/config"
	"github.com/the-Data-Appeal-Company/metaman/pkg/deleter"
	"github.com/the-Data-Appeal-Company/metaman/pkg/manager"
	"github.com/the-Data-Appeal-Company/metaman/pkg/metastore"
	"log"
)

var ConfPath string

var rootCmd = &cobra.Command{
	Use:   "metaman",
	Short: "metaman is the command-line tool/api to interact with metastore",
	Long: `metaman is the command-line tool/api to interact with metastore.
Currently supported metastore are: Glue, Hive.
Supported operations are:
- create tables
- drop tables along with data
- sync different metastore`,
}

func init() {
	formatter := &logrus.TextFormatter{
		FullTimestamp: true,
	}
	logrus.SetFormatter(formatter)

	rootCmd.PersistentFlags().StringVarP(&ConfPath, "config", "c", "config.yml", "Configuration path default ./config.yml")

	rootCmd.AddCommand(dropCmd)
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(syncCmd)
	rootCmd.AddCommand(apiCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func createAwsSession(conf metamanConf.Aws) *session.Session {
	return session.Must(session.NewSession(&aws.Config{
		Region: aws.String(conf.Region),
	}))
}

func getMetastoreManager() (*manager.HiveGlueManager, error) {
	configuration, err := metamanConf.FromYaml(ConfPath)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	s3Client, err := getS3Client(ctx, configuration)
	if err != nil {
		return nil, err
	}
	fileDeleter := deleter.NewFileDeleterS3(s3Client)
	sess := createAwsSession(configuration.Aws)
	if err != nil {
		return nil, err
	}
	clientHive, err := hmsclient.Open(configuration.Metastore.Hive.Url, configuration.Metastore.Hive.Port)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(configuration.Db.Driver, configuration.Db.ConnectionString())
	if err != nil {
		return nil, err
	}
	aux := metastore.NewPgAuxInfoRetriever(db)
	pool := metastore.NewPoolMetastore(
		metastore.NewHiveMetaStore(clientHive, fileDeleter, aux),
		metastore.NewGlueMetaStore(awsGlue.New(sess), fileDeleter),
	)
	return manager.NewHiveGlueManager(pool), nil
}

func getS3Client(ctx context.Context, configuration metamanConf.Conf) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(configuration.Aws.Region))
	if err != nil {
		return nil, err
	}
	s3Client := s3.NewFromConfig(cfg)
	return s3Client, nil
}

func mapMetastoreCode(name string) (metastore.MetastoreCode, error) {
	var code metastore.MetastoreCode
	switch metastore.MetastoreCode(name) {
	case metastore.GLUE:
		code = metastore.GLUE
	case metastore.HIVE:
		code = metastore.HIVE
	default:
		return "", fmt.Errorf("metastore %s not supported", name)
	}
	return code, nil
}
