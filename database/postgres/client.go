package pg

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/newrelic/go-agent/v3/integrations/nrpgx"
)

type Config struct {
	User               string
	Host               string
	Password           string
	Port               int
	DbName             string
	MaxOpenConnections int
	MaxIdleConnections int
}

// Get a DB connection pool using username/password credentials
func NewWithUsernameAndPassword(username, password, hostname, port, dbname string) (*sql.DB, error) {
	// IMPORTANT: Supported by Aurora Serverless clusters

	// Use password based authentication
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		username, password, hostname, port, dbname,
	)

	// Try to open a connection pool using the "pgx" driver (instead of "postgres")
	db, err := sql.Open("nrpgx", dsn)
	if err != nil {
		return nil, err
	}

	// Check if the connection was successful
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}
