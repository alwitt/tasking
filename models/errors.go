// Package models - system data models
package models

import "github.com/alwitt/goutils"

// ======================================================================================
// Persistence Layer Errors - SQL

// PersistenceError encountered when operating the persistence layer (e.g. SQL statement failed)
//
// Not recoverable
type PersistenceError struct{ goutils.BaseError }

// NewPersistenceError builds a PersistenceError, optionally capturing the call stack.
func NewPersistenceError(message string, core error, getCallStack bool) PersistenceError {
	base := goutils.BaseError{Name: "PersistenceError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return PersistenceError{BaseError: base}
}

// SQLError wraps an error returned by the GORM layer, indicating a SQL statement failed
type SQLError struct{ goutils.BaseError }

// NewSQLError builds a SQLError, optionally capturing the call stack.
func NewSQLError(message string, core error, getCallStack bool) SQLError {
	base := goutils.BaseError{Name: "SQLError", Message: message, Core: core}
	if getCallStack {
		base.Stack = goutils.GetCallStack(1)
	}
	return SQLError{BaseError: base}
}
