// Package storage is an SQL-backed storage implementation for go-git.
package storage

import (
	"database/sql"

	"github.com/go-git/go-git/v5/storage/memory"
)

type Storage struct {
	ObjectStorage
	ReferenceStorage

	// The following structs are duplicated from the go-git memory storage
	// implementation, however they are omitted from the declaration below.
	memory.ConfigStorage
	memory.IndexStorage
	memory.ModuleStorage
	memory.ShallowStorage
}

// NewStorage creates a new SQL-backed storage interface.
func NewStorage(client *sql.DB) *Storage {
	return &Storage{
		ReferenceStorage: ReferenceStorage{client: client},
		ObjectStorage:    ObjectStorage{client: client},
	}
}
