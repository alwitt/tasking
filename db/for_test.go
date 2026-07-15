// Package db - database controllers for system persistence
package db

import (
	"context"

	"gorm.io/gorm"
)

// DefineTables helper function meant to be used for unit-testing to prepare a
// database with tables
func DefineTables(_ context.Context, db *gorm.DB) error {
	return db.AutoMigrate(
		&taskEntry{},
		&taskExecutionEntry{},
		&systemEventAuditEntry{},
	)
}
