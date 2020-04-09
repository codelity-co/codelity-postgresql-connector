package connector

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type PostgresqlConnector struct {
	ServerUrls string
	Database *sql.DB
	Transaction *sql.Tx
	TableName string
	AttrsColumnName string
}

type Attrs map[string]interface{}

func (a Attrs) Value() (driver.Value, error) {
    return json.Marshal(a)
}

func(c *PostgresqlConnector) Connect() error {
	if db, err := sql.Open("postgres", c.ServerUrls); err != nil {
		return err
	} else {
		c.Database = db
	}
	return nil
}

func(c *PostgresqlConnector) Close () error {
	return c.Database.Close()
}

func(c *PostgresqlConnector) BeginTransaction() error {
	txn, err := c.Database.Begin()
	if err != nil {
		return err
	}
	c.Transaction = txn
	return nil
}

func(c *PostgresqlConnector) CommitTransaction() error {
	return c.Transaction.Commit()
}

func(c *PostgresqlConnector) RollbackTransaction() error {
	return c.Transaction.Rollback()
}

func(c *PostgresqlConnector) InsertJsonRecord(attrs Attrs) error {
	_, err := c.Transaction.Exec("INSERT INTO $1 ($2) VALUES ($3)", c.TableName, c.AttrsColumnName, attrs)
	return err
}

func(c *PostgresqlConnector) UpdateJsonRecord(id string, attrs Attrs) error {
	_, err := c.Transaction.Exec("UPDATE $1 SET $2 = $4 WHERE ID=$4", c.TableName, c.AttrsColumnName, attrs, id)
	return err
}

func(c *PostgresqlConnector) DeleteJsonRecord(id string) error {
	_, err := c.Transaction.Exec("DELETE FROM $1 WHERE ID=$2", c.TableName, id)
	return err	
}