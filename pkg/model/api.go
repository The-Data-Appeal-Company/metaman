package model

type CreateApiRequest struct {
	Metastores []string         `json:"metastores"`
	Tables     []DatabaseTables `json:"tables"`
}

type DropApiRequest struct {
	Metastore string    `json:"metastore"`
	Tables    []DropArg `json:"tables"`
}

type SyncApiRequest struct {
	Source string   `json:"source"`
	Target string   `json:"target"`
	DbName string   `json:"db"`
	Tables []string `json:"tables"`
	Delete bool     `json:"delete"`
}
