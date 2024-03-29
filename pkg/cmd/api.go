package cmd

import (
	ginprometheus "github.com/banzaicloud/go-gin-prometheus"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/the-Data-Appeal-Company/metaman/pkg/config"
	"github.com/the-Data-Appeal-Company/metaman/pkg/manager"
	"github.com/the-Data-Appeal-Company/metaman/pkg/model"
	"net/http"
)

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "run an api for metastore management",
	Long:  `api have the same functionality as the command line but served as rest api`,
	RunE:  api,
}

func api(cmd *cobra.Command, args []string) error {
	configuration, err := config.FromYaml(ConfPath)
	if err != nil {
		return err
	}
	metaman, err := getMetastoreManager()
	if err != nil {
		return err
	}

	handler := ApiHandler{manager: metaman}
	router := handler.setupRouter()
	if configuration.Prometheus.Enabled {
		p := ginprometheus.NewPrometheus("gin", []string{})
		p.Use(router, "/metrics")
	}
	logrus.Info("starting MetaMan API")
	return router.Run()
}

type ApiHandler struct {
	manager manager.Manager
}

func (a *ApiHandler) setupRouter() *gin.Engine {
	router := gin.New()

	router.Use(gin.Recovery())

	router.POST("/create", a.handleCreate)
	router.DELETE("/drop", a.handleDrop)
	router.PUT("/sync", a.handleSync)
	router.GET("/healthcheck", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "UP",
		})
	})
	return router
}

func (a *ApiHandler) handleSync(c *gin.Context) {
	var request model.SyncApiRequest
	err := c.BindJSON(&request)
	if err != nil {
		logrus.Warnf("sync bad request: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err,
		})
		return
	}
	source, err := mapMetastoreCode(request.Source)
	if err != nil {
		logrus.Warnf("sync bad request: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err,
		})
		return
	}
	target, err := mapMetastoreCode(request.Target)
	if err != nil {
		logrus.Warnf("sync bad request: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err,
		})
		return
	}
	err = a.manager.Sync(source, target, request.DbName, request.Tables, request.Delete)
	if err != nil {
		logrus.Errorf("sync error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.Status(http.StatusOK)
}

func (a *ApiHandler) handleDrop(c *gin.Context) {
	var request model.DropApiRequest
	err := c.BindJSON(&request)
	if err != nil {
		logrus.Warnf("drop bad request: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err,
		})
		return
	}
	code, err := mapMetastoreCode(request.Metastore)
	if err != nil {
		logrus.Warnf("drop bad request: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err,
		})
		return
	}
	errs := a.manager.Drop(code, request.Tables)
	if errs != nil {
		logrus.Errorf("drop error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"errors": errorsAsStrings(errs),
		})
		return
	}
	c.Status(http.StatusOK)
}

func (a *ApiHandler) handleCreate(c *gin.Context) {
	var request model.CreateApiRequest
	err := c.BindJSON(&request)
	if err != nil {
		logrus.Warnf("create bad request: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err,
		})
		return
	}
	codes, err := mapMetastoreCodes(request.Metastores)
	if err != nil {
		logrus.Warnf("create bad request: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err,
		})
		return
	}
	err = a.manager.Create(codes, request.Tables)
	if err != nil {
		logrus.Errorf("create error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.Status(http.StatusOK)
}

func errorsAsStrings(errs []error) []string {
	errStrings := make([]string, len(errs))
	for i, err := range errs {
		errStrings[i] = err.Error()
	}
	return errStrings
}
