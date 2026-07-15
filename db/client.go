// Package db - database controllers for system persistence
package db

import (
	"context"
	"fmt"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

/*
GetSqliteDialector define Sqlite GORM dialector

	@param dbFile string - Sqlite DB file
	@return GORM sqlite dialector
*/
func GetSqliteDialector(dbFile string) gorm.Dialector {
	return sqlite.Open(fmt.Sprintf("%s?_foreign_keys=on", dbFile))
}

// Client manages connections and transactions with a DB
type Client interface {
	/*
		RunSQLInTransaction execute SQL calls within a transaction

			@param ctx context.Context - execution context
			@param coreLogic func(ctx context.Context, tx *gorm.DB) error - the callback to execute
	*/
	RunSQLInTransaction(
		ctx context.Context, coreLogic func(ctx context.Context, tx *gorm.DB) error,
	) error

	/*
		RunSQLInTransaction utilize a `Database` instance in a transaction

			@param ctx context.Context - execution context
			@param coreLogic func(ctx context.Context, dbClient Database) error - the callback to execute
	*/
	UseDatabaseInTransaction(
		ctx context.Context, coreLogic func(ctx context.Context, dbClient Database) error,
	) error
}

// clientImpl implements Client
type clientImpl struct {
	goutils.Component
	db *gorm.DB
}

/*
NewConnection define a new SQL client

	@param dbDialector gorm.Dialector - GORM dialector
	@param dbLogLevel logger.LogLevel - SQL log level
	@return new client
*/
func NewConnection(dbDialector gorm.Dialector, dbLogLevel logger.LogLevel) (Client, error) {
	logTags := log.Fields{"package": "tasking", "module": "db", "component": "sql-client"}

	db, err := gorm.Open(dbDialector, &gorm.Config{
		Logger:                 logger.Default.LogMode(dbLogLevel),
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, goutils.NewRuntimeError("failed to connect with DB", err, true)
	}

	instance := &clientImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		db: db,
	}

	return instance, nil
}

/*
RunSQLInTransaction execute SQL calls within a transaction

	@param ctx context.Context - execution context
	@param coreLogic func(ctx context.Context, tx *gorm.DB) error - the callback to execute
*/
func (c *clientImpl) RunSQLInTransaction(
	ctx context.Context, coreLogic func(ctx context.Context, tx *gorm.DB) error,
) error {
	return c.db.Transaction(func(tx *gorm.DB) error {
		return coreLogic(ctx, tx)
	})
}

/*
RunSQLInTransaction utilize a `Database` instance in a transaction

	@param ctx context.Context - execution context
	@param coreLogic func(ctx context.Context, dbClient Database) error - the callback to execute
*/
func (c *clientImpl) UseDatabaseInTransaction(
	ctx context.Context, coreLogic func(ctx context.Context, dbClient Database) error,
) error {
	return c.RunSQLInTransaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
		dbClient, err := newDatabase(ctx, tx)
		if err != nil {
			return goutils.NewRuntimeError("failed to define `Database` instance", err, true)
		}
		return coreLogic(ctx, dbClient)
	})
}

/*
ActiveSessionWrapper helper function for deciding whether to start a new transition
or use an existing one.

	@param ctx context.Context - execution context
	@param activeDBClient Database - existing database transaction
	@param persistence Client - persistence client
	@param coreLogic func(ctx context.Context, dbClient Database) error - the callback to execute
*/
func ActiveSessionWrapper(
	ctx context.Context,
	activeDBClient Database,
	persistence Client,
	coreLogic func(ctx context.Context, dbClient Database) error,
) error {
	if activeDBClient == nil {
		return persistence.UseDatabaseInTransaction(ctx, coreLogic)
	}
	return coreLogic(ctx, activeDBClient)
}
