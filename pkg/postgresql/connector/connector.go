package connector

import (
	"fmt"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/lib/pq"
	"github.com/google/uuid"
)

type PostgresqlConnector struct {
	Dsn string
	ConnectionOptions map[string]interface{}
	TableName string
	Database *gorm.DB
}

type AttrsType map[string]interface{}

type JsonRecord struct {
	ID uuid.UUID `gorm:"type:uuid;column:ID;primary_key;"`
	Attrs AttrsType `gorm:"type:json;column:Attrs;"`

	table string `gorm:"-"`
}

func (entity *JsonRecord) BeforeCreate(scope *gorm.Scope) error {
	uuid, err := uuid.NewUUID()
	if err != nil {
	 return err
	}
	return scope.SetColumn("ID", uuid)
 }

func(c *PostgresqlConnector) Connect() error {
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

func(c *PostgresqlConnector) Close () error {
	return c.Database.Close()
}

func(c *PostgresqlConnector) AutoMigrate() error {
	return c.Database.AutoMigrate(&JsonRecord{table: c.TableName}).Error
}

func(c *PostgresqlConnector) BeginTransaction() (*gorm.DB, error) {
	txn := c.Database.Begin()
	if txn == nil {
		return nil, fmt.Errorf("Cannot start database transaction")
	}
	return txn, nil
}

func(c *PostgresqlConnector) CommitTransaction(txn *gorm.DB) error {
	return txn.Commit().Error
}

func(c *PostgresqlConnector) RollbackTransaction(txn *gorm.DB) {
	txn.Rollback()
}

func(c *PostgresqlConnector) CreateJsonRecord(txn *gorm.DB, jsonRecord *JsonRecord) error {
	return txn.Create(&jsonRecord).Error
}

func(c *PostgresqlConnector) UpdateJsonRecord(txn *gorm.DB, jsonRecord *JsonRecord) error {
	return txn.Save(&jsonRecord).Error
}

func(c *PostgresqlConnector) DeleteJsonRecord(txn *gorm.DB, jsonRecord *JsonRecord) error {
	return txn.Delete(&jsonRecord).Error
}