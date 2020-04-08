package connector

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type CockroachdbConnector struct {
	ServerUrls string
	Database *sql.DB
	Transaction *sql.Tx
}

type Item struct {
	ID    string
	Attrs Attrs
}

type Attrs map[string]interface{}

func (a Attrs) Value() (driver.Value, error) {
    return json.Marshal(a)
}

func(c *CockroachdbConnector) Connect() error {
	if db, err := sql.Open("postgres", c.ServerUrls); err != nil {
		return err
	} else {
		c.Database = db
	}
	return nil
}

func(c *CockroachdbConnector) Close () error {
	return c.Database.Close()
}

func(c *CockroachdbConnector) BeginTransaction() error {
	txn, err := c.Database.Begin()
	if err != nil {
		return err
	}
	c.Transaction = txn
	return nil
}

func(c *CockroachdbConnector) CommitTransaction() error {
	return c.Transaction.Commit()
}

func(c *CockroachdbConnector) RollbackTransaction() error {
	return c.Transaction.Rollback()
}

func(c *CockroachdbConnector) InsertJsonRecord(tableName string, columnName string, item Item) error {
	if len(item.ID) == 0 {
		item.ID = uuid.New().String()
	}
	_, err := c.Transaction.Exec("INSERT INTO $1 VALUES ($2, $3)", tableName, item.ID, item.Attrs)
	return err
}

func(c *CockroachdbConnector) UpdateJsonRecord(tableName string, item Item) error {
	_, err := c.Transaction.Exec("UPDATE $1 SET Attrs = $2 WHERE ID=$3", tableName, item.Attrs, item.ID)
	return err
}

func(c *CockroachdbConnector) DeleteJsonRecord(tableName string, item Item) error {
	_, err := c.Transaction.Exec("DELETE FROM $1 WHERE ID=$2", tableName, item.ID)
	return err	
}