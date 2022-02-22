package metastore

import (
	"context"
	"database/sql"
	"fmt"
)

type AuxInfoRetriever interface {
	GetTableProperty(ctx context.Context, table, tableParam string) (string, error)
}

type PgAuxInfoRetriever struct {
	db *sql.DB
}

func NewPgAuxInfoRetriever(db *sql.DB) *PgAuxInfoRetriever {
	return &PgAuxInfoRetriever{db: db}
}

func (p *PgAuxInfoRetriever) GetTableProperty(ctx context.Context, table, tableParam string) (string, error) {
	row := p.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT "PARAM_VALUE"
			FROM "TBLS" t JOIN
     		"TABLE_PARAMS" tp ON t."TBL_ID" = tp."TBL_ID"
			WHERE t."TBL_NAME" = '%s'
			AND "PARAM_KEY" = '%s'`, table, tableParam))
	if row.Err() != nil {
		return "", row.Err()
	}
	var paramValue string
	err := row.Scan(&paramValue)
	if err != nil {
		return "", err
	}
	return paramValue, nil
}
