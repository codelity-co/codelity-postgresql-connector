package connector

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/lib/pq"
)

type PostgresqlConnector struct {
	Dsn               string
	ConnectionOptions map[string]interface{}
	TableName         string
	Database          *gorm.DB
}

type AttrsType map[string]interface{}

func (at AttrsType) Value() (driver.Value, error) {
	j, err := json.Marshal(at)
	return j, err
}

func (p *AttrsType) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("Type assertion .([]byte) failed.")
	}

	var i interface{}
	if err := json.Unmarshal(source, &i); err != nil {
		return err
	}

	*p, ok = i.(map[string]interface{})
	if !ok {
		return errors.New("Type assertion .(map[string]interface{}) failed.")
	}

	return nil
}

type JsonRecord struct {
	ID    uuid.UUID `gorm:"type:uuid;column:ID;primary_key;"`
	Attrs AttrsType `gorm:"type:json;not null;default '{}'"`

	table string `gorm:"-"`
}

func (r JsonRecord) TableName() string {
	if r.table != "" {
		return r.table
	}
	return "json_records"
}

func (r *JsonRecord) BeforeCreate(scope *gorm.Scope) error {
	uuid, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	return scope.SetColumn("ID", uuid)
}

func (c *PostgresqlConnector) Connect() error {
	var connectString string
	if len(c.Dsn) > 0 {
		connectString = c.Dsn
	} else {
		for k, v := range c.ConnectionOptions {
			if len(connectString) > 0 {
				connectString = connectString + " "
			}
			connectString = connectString + fmt.Sprintf("%v=%v", k, v) //nolint:govet
		}
	}

	if db, err := gorm.Open("postgres", connectString); err != nil {
		return err
	} else {
		c.Database = db
	}
	return nil
}

func (c *PostgresqlConnector) Close() error {
	return c.Database.Close()
}

func (c *PostgresqlConnector) AutoMigrate() error {
	return c.Database.AutoMigrate(&JsonRecord{table: c.TableName}).Error
}

func (c *PostgresqlConnector) BeginTransaction() (*gorm.DB, error) {
	txn := c.Database.Begin()
	if txn == nil {
		return nil, fmt.Errorf("Cannot start database transaction")
	}
	return txn, nil
}

func (c *PostgresqlConnector) CommitTransaction(txn *gorm.DB) error {
	return txn.Commit().Error
}

func (c *PostgresqlConnector) RollbackTransaction(txn *gorm.DB) {
	txn.Rollback()
}

func (c *PostgresqlConnector) CreateJsonRecord(txn *gorm.DB, jsonRecord *JsonRecord) error {
	jsonRecord.table = c.TableName
	return txn.Create(&jsonRecord).Error
}

func (c *PostgresqlConnector) UpdateJsonRecord(txn *gorm.DB, jsonRecord *JsonRecord) error {
	jsonRecord.table = c.TableName
	return txn.Save(&jsonRecord).Error
}

func (c *PostgresqlConnector) DeleteJsonRecord(txn *gorm.DB, jsonRecord *JsonRecord) error {
	jsonRecord.table = c.TableName
	return txn.Delete(&jsonRecord).Error
}
